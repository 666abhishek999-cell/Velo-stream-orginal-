package com.example.audio

import android.annotation.SuppressLint
import android.media.AudioAttributes
import android.media.AudioFormat
import android.media.AudioPlaybackCaptureConfiguration
import android.media.AudioRecord
import android.media.MediaCodec
import android.media.MediaCodecInfo
import android.media.MediaFormat
import android.media.MediaRecorder
import android.media.projection.MediaProjection
import android.os.Build
import android.util.Log
import com.example.model.AudioSourceType
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.concurrent.atomic.AtomicBoolean

class AudioCaptureEngine(
    private val audioSourceType: AudioSourceType,
    private val audioBitrateKbps: Int = 128,
    private val micVolumeMultiplier: Float = 1.0f,
    private val internalVolumeMultiplier: Float = 1.0f,
    private val mediaProjection: MediaProjection?,
    private val onAudioFormatConfigured: (MediaFormat, ByteArray) -> Unit,
    private val onAudioSampleEncoded: (ByteArray, Long) -> Unit,
    private val onError: (String) -> Unit
) {
    companion object {
        private const val TAG = "AudioCaptureEngine"
        private const val SAMPLE_RATE = 44100
        private const val CHANNEL_CONFIG = AudioFormat.CHANNEL_IN_STEREO
        private const val AUDIO_FORMAT = AudioFormat.ENCODING_PCM_16BIT
        private const val CHANNELS = 2
    }

    private val isRunning = AtomicBoolean(false)
    private var micRecord: AudioRecord? = null
    private var internalRecord: AudioRecord? = null
    private var audioEncoder: MediaCodec? = null

    private var workerThread: Thread? = null

    @SuppressLint("MissingPermission")
    fun start(): Boolean {
        if (audioSourceType == AudioSourceType.MUTED) {
            Log.d(TAG, "Audio is muted by configuration.")
            return true
        }

        val minBufSize = AudioRecord.getMinBufferSize(SAMPLE_RATE, CHANNEL_CONFIG, AUDIO_FORMAT)
        val bufferSize = maxOf(minBufSize, 4096 * 4)

        // Setup microphone if requested
        if (audioSourceType == AudioSourceType.MICROPHONE || audioSourceType == AudioSourceType.INTERNAL_PLUS_MIC) {
            try {
                micRecord = AudioRecord(
                    MediaRecorder.AudioSource.MIC,
                    SAMPLE_RATE,
                    CHANNEL_CONFIG,
                    AUDIO_FORMAT,
                    bufferSize
                )
                if (micRecord?.state != AudioRecord.STATE_INITIALIZED) {
                    micRecord?.release()
                    micRecord = null
                    Log.w(TAG, "Microphone AudioRecord failed to initialize")
                }
            } catch (e: Exception) {
                Log.w(TAG, "Failed to create microphone AudioRecord: ${e.message}")
            }
        }

        // Setup internal audio if requested
        if (audioSourceType == AudioSourceType.INTERNAL_AUDIO || audioSourceType == AudioSourceType.INTERNAL_PLUS_MIC) {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q && mediaProjection != null) {
                try {
                    val config = AudioPlaybackCaptureConfiguration.Builder(mediaProjection)
                        .addMatchingUsage(AudioAttributes.USAGE_MEDIA)
                        .addMatchingUsage(AudioAttributes.USAGE_GAME)
                        .addMatchingUsage(AudioAttributes.USAGE_UNKNOWN)
                        .build()

                    internalRecord = AudioRecord.Builder()
                        .setAudioFormat(
                            AudioFormat.Builder()
                                .setEncoding(AUDIO_FORMAT)
                                .setSampleRate(SAMPLE_RATE)
                                .setChannelMask(CHANNEL_CONFIG)
                                .build()
                        )
                        .setBufferSizeInBytes(bufferSize)
                        .setAudioPlaybackCaptureConfig(config)
                        .build()

                    if (internalRecord?.state != AudioRecord.STATE_INITIALIZED) {
                        internalRecord?.release()
                        internalRecord = null
                        Log.w(TAG, "Internal AudioRecord failed to initialize")
                    }
                } catch (e: Exception) {
                    Log.w(TAG, "Failed to setup AudioPlaybackCapture: ${e.message}")
                }
            } else {
                Log.w(TAG, "Internal audio capture requires Android 10+ and an active MediaProjection session.")
                if (audioSourceType == AudioSourceType.INTERNAL_AUDIO) {
                    onError("Internal audio capture is only supported on Android 10+ with active capture authorization.")
                }
            }
        }

        if (micRecord == null && internalRecord == null) {
            onError("Unable to initialize requested audio source(s). Check microphone permissions or Android version.")
            return false
        }

        // Initialize AAC Encoder
        try {
            val format = MediaFormat.createAudioFormat(MediaFormat.MIMETYPE_AUDIO_AAC, SAMPLE_RATE, CHANNELS).apply {
                setInteger(MediaFormat.KEY_AAC_PROFILE, MediaCodecInfo.CodecProfileLevel.AACObjectLC)
                setInteger(MediaFormat.KEY_BIT_RATE, audioBitrateKbps * 1000)
                setInteger(MediaFormat.KEY_MAX_INPUT_SIZE, 16384)
            }

            audioEncoder = MediaCodec.createEncoderByType(MediaFormat.MIMETYPE_AUDIO_AAC).apply {
                configure(format, null, null, MediaCodec.CONFIGURE_FLAG_ENCODE)
                start()
            }
        } catch (e: Exception) {
            Log.e(TAG, "Failed to initialize AAC audio encoder: ${e.message}")
            onError("Failed to start AAC encoder: ${e.message}")
            release()
            return false
        }

        isRunning.set(true)
        micRecord?.startRecording()
        internalRecord?.startRecording()

        startAudioLoop(bufferSize)
        return true
    }

    private fun startAudioLoop(bufferSize: Int) {
        workerThread = Thread({
            val micShorts = ShortArray(bufferSize / 2)
            val intShorts = ShortArray(bufferSize / 2)
            val mixedShorts = ShortArray(bufferSize / 2)
            val mixedBytes = ByteArray(bufferSize)

            var startTimeNs = System.nanoTime()
            val bufferInfo = MediaCodec.BufferInfo()

            while (isRunning.get()) {
                val hasMic = micRecord != null
                val hasInt = internalRecord != null

                var micReadCount = 0
                var intReadCount = 0

                if (hasMic) {
                    micReadCount = micRecord?.read(micShorts, 0, micShorts.size) ?: 0
                }
                if (hasInt) {
                    intReadCount = internalRecord?.read(intShorts, 0, intShorts.size) ?: 0
                }

                val samplesCount = maxOf(maxOf(micReadCount, 0), maxOf(intReadCount, 0))
                if (samplesCount <= 0) {
                    Thread.sleep(10)
                    continue
                }

                // Mix samples with volume scaling and clipping protection
                for (i in 0 until samplesCount) {
                    val micSample = if (i < micReadCount) (micShorts[i] * micVolumeMultiplier).toInt() else 0
                    val intSample = if (i < intReadCount) (intShorts[i] * internalVolumeMultiplier).toInt() else 0
                    val mixed = micSample + intSample
                    mixedShorts[i] = mixed.coerceIn(-32768, 32767).toShort()
                }

                // Convert shorts to 16-bit PCM bytes (little endian)
                val byteCount = samplesCount * 2
                ByteBuffer.wrap(mixedBytes).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer().put(mixedShorts, 0, samplesCount)

                // Feed into MediaCodec
                val encoder = audioEncoder ?: break
                val inputIndex = encoder.dequeueInputBuffer(5000)
                if (inputIndex >= 0) {
                    val inputBuf = encoder.getInputBuffer(inputIndex)
                    if (inputBuf != null) {
                        inputBuf.clear()
                        inputBuf.put(mixedBytes, 0, byteCount)
                        val presentationTimeUs = System.nanoTime() / 1000
                        encoder.queueInputBuffer(inputIndex, 0, byteCount, presentationTimeUs, 0)
                    }
                }

                // Drain output from MediaCodec
                drainEncoder(encoder, bufferInfo)
            }
        }, "VeloStream-AudioLoop").apply {
            priority = Thread.NORM_PRIORITY + 2
            start()
        }
    }

    private fun drainEncoder(encoder: MediaCodec, bufferInfo: MediaCodec.BufferInfo) {
        while (isRunning.get()) {
            val outputIndex = encoder.dequeueOutputBuffer(bufferInfo, 0)
            if (outputIndex >= 0) {
                val outputBuffer = encoder.getOutputBuffer(outputIndex)
                if (outputBuffer != null && bufferInfo.size > 0) {
                    outputBuffer.position(bufferInfo.offset)
                    outputBuffer.limit(bufferInfo.offset + bufferInfo.size)

                    val isConfig = (bufferInfo.flags and MediaCodec.BUFFER_FLAG_CODEC_CONFIG) != 0
                    val bytes = ByteArray(bufferInfo.size)
                    outputBuffer.get(bytes)

                    if (isConfig) {
                        Log.d(TAG, "Audio encoder output config: ${bytes.size} bytes")
                        val format = encoder.outputFormat
                        onAudioFormatConfigured(format, bytes)
                    } else {
                        val ptsMs = bufferInfo.presentationTimeUs / 1000
                        onAudioSampleEncoded(bytes, ptsMs)
                    }
                }
                encoder.releaseOutputBuffer(outputIndex, false)
            } else if (outputIndex == MediaCodec.INFO_OUTPUT_FORMAT_CHANGED) {
                val format = encoder.outputFormat
                val csd0 = format.getByteBuffer("csd-0")
                if (csd0 != null) {
                    val asc = ByteArray(csd0.remaining())
                    csd0.get(asc)
                    csd0.rewind()
                    onAudioFormatConfigured(format, asc)
                }
            } else {
                break
            }
        }
    }

    fun stop() {
        isRunning.set(false)
        try {
            workerThread?.interrupt()
            workerThread?.join(500)
        } catch (_: Exception) {}
        workerThread = null
        release()
    }

    private fun release() {
        try {
            micRecord?.stop()
            micRecord?.release()
        } catch (_: Exception) {}
        micRecord = null

        try {
            internalRecord?.stop()
            internalRecord?.release()
        } catch (_: Exception) {}
        internalRecord = null

        try {
            audioEncoder?.stop()
            audioEncoder?.release()
        } catch (_: Exception) {}
        audioEncoder = null
    }
}
