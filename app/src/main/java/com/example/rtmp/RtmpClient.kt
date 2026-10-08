package com.example.rtmp

import android.util.Log
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.net.InetSocketAddress
import java.net.Socket
import java.net.URI
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.security.SecureRandom
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import javax.net.ssl.SSLSocket
import javax.net.ssl.SSLSocketFactory

class RtmpClient(
    private val onStatusChanged: (String) -> Unit = {},
    private val onError: (String) -> Unit = {}
) {
    companion object {
        private const val TAG = "RtmpClient"
        private const val DEFAULT_RTMP_PORT = 1935
        private const val DEFAULT_RTMPS_PORT = 443
        private const val CHUNK_SIZE = 4096
        private const val MAX_QUEUE_SIZE = 80

        // Packet types
        const val TYPE_SET_CHUNK_SIZE = 0x01
        const val TYPE_ABORT_MESSAGE = 0x02
        const val TYPE_ACKNOWLEDGEMENT = 0x03
        const val TYPE_USER_CONTROL = 0x04
        const val TYPE_WINDOW_ACK_SIZE = 0x05
        const val TYPE_SET_PEER_BANDWIDTH = 0x06
        const val TYPE_AUDIO = 0x08
        const val TYPE_VIDEO = 0x09
        const val TYPE_DATA = 0x12
        const val TYPE_COMMAND_AMF0 = 0x14

        // CSIDs
        const val CSID_CONTROL = 2
        const val CSID_COMMAND = 3
        const val CSID_AUDIO = 4
        const val CSID_VIDEO = 6
    }

    private val writeLock = Any()
    private var socket: Socket? = null
    private var inStream: InputStream? = null
    private var outStream: OutputStream? = null

    private val isConnected = AtomicBoolean(false)
    private val isPublishing = AtomicBoolean(false)
    private var streamId = 1
    private var serverChunkSize = 128

    private val packetQueue = LinkedBlockingQueue<RtmpPacket>(MAX_QUEUE_SIZE)
    private var senderThread: Thread? = null

    // Stats
    val droppedFramesCount = AtomicLong(0)
    val totalBytesSent = AtomicLong(0)
    private var lastSpeedCheckTime = System.currentTimeMillis()
    private var lastBytesCount = 0L
    @Volatile var currentUploadSpeedKbps: Long = 0L
        private set

    private var startTimeMs: Long = 0

    fun isLive(): Boolean = isPublishing.get()

    /**
     * Connects to RTMP or RTMPS server and begins publishing.
     */
    fun connectAndPublish(serverUrl: String, streamKey: String): Boolean {
        try {
            onStatusChanged("Connecting to server…")
            val target = parseUrl(serverUrl, streamKey)
            Log.d(TAG, "Parsed destination: host=${target.host}, port=${target.port}, app=${target.app}, tcUrl=${target.tcUrl}, isSsl=${target.isSsl}")

            val rawSocket = createAndConnectSocket(target, 6000)
            socket = rawSocket
            inStream = BufferedInputStream(rawSocket.getInputStream(), 32 * 1024)
            outStream = BufferedOutputStream(rawSocket.getOutputStream(), 64 * 1024)

            onStatusChanged("Handshaking…")
            doHandshake()

            onStatusChanged("Configuring chunk stream…")
            sendChunkSize(CHUNK_SIZE)

            onStatusChanged("Connecting AMF0…")
            sendConnectCommand(target.app, target.tcUrl)
            
            val connectResult = readServerMessagesUntilCommand(targetCommand = null, timeoutMs = 6000)
            Log.d(TAG, "Connect result: name=${connectResult?.name}, obj=${connectResult?.infoObj}")

            if (connectResult?.name == "_error") {
                val desc = extractErrorDescription(connectResult.infoObj)
                throw IllegalStateException("Connect rejected: $desc")
            }

            sendReleaseStream(target.streamKey)
            sendFCPublish(target.streamKey)
            sendCreateStream()
            
            val createStreamResult = readServerMessagesUntilCommand(targetCommand = null, timeoutMs = 6000)
            Log.d(TAG, "CreateStream response: name=${createStreamResult?.name}, streamId=${createStreamResult?.infoObj}")
            
            if (createStreamResult != null && createStreamResult.infoObj is Number) {
                val returnedId = (createStreamResult.infoObj as Number).toInt()
                if (returnedId > 0) {
                    streamId = returnedId
                }
            }

            onStatusChanged("Publishing…")
            sendPublish(target.streamKey)

            isConnected.set(true)
            isPublishing.set(true)
            startTimeMs = System.currentTimeMillis()

            startSenderThread()
            onStatusChanged("Live")
            Log.d(TAG, "RTMP publish successfully started on streamId $streamId")
            return true
        } catch (e: Exception) {
            Log.e(TAG, "Connection failed: ${e.message}", e)
            disconnect()
            onError(e.message ?: "Connection failed")
            return false
        }
    }

    private fun extractErrorDescription(obj: Any?): String {
        if (obj is Map<*, *>) {
            return (obj["description"] ?: obj["code"] ?: "Unknown error").toString()
        }
        return obj?.toString() ?: "Connection rejected"
    }

    private fun startSenderThread() {
        senderThread = Thread({
            while (isPublishing.get()) {
                try {
                    val packet = packetQueue.take()
                    val out = outStream ?: break

                    sendRtmpMessage(
                        csid = if (packet.type == TYPE_AUDIO) CSID_AUDIO else CSID_VIDEO,
                        messageType = packet.type,
                        timestamp = packet.timestamp,
                        streamId = streamId,
                        payload = packet.payload,
                        out = out
                    )

                    val bytes = packet.payload.size.toLong()
                    totalBytesSent.addAndGet(bytes)
                    updateSpeed()
                } catch (_: InterruptedException) {
                    break
                } catch (e: Exception) {
                    Log.e(TAG, "Send error: ${e.message}")
                    if (isPublishing.get()) {
                        onError("Streaming error: ${e.message}")
                    }
                    break
                }
            }
        }, "VeloStream-RTMP-Sender").apply {
            priority = Thread.MAX_PRIORITY
            start()
        }
    }

    private fun updateSpeed() {
        val now = System.currentTimeMillis()
        val delta = now - lastSpeedCheckTime
        if (delta >= 1000) {
            val total = totalBytesSent.get()
            val bytesInSec = total - lastBytesCount
            currentUploadSpeedKbps = (bytesInSec * 8) / 1000
            lastBytesCount = total
            lastSpeedCheckTime = now
        }
    }

    /**
     * Queues H.264 or Enhanced RTMP H.265 (HEVC) video packet with proper FLV video tag header.
     */
    fun sendVideo(payload: ByteArray, timestampMs: Long, isKeyframe: Boolean, isHevc: Boolean = false) {
        if (!isPublishing.get()) return

        // If queue backs up, drop non-keyframe to keep gaming low latency
        if (packetQueue.size > 35 && !isKeyframe) {
            droppedFramesCount.incrementAndGet()
            return
        }

        val flvPayload = if (!isHevc) {
            // RTMP Video Message requires 5-byte FLV video tag header for H.264 (AVC):
            // Byte 0: FrameType (1=keyframe, 2=inter) << 4 | CodecID (7=AVC) -> 0x17 for keyframe, 0x27 for inter
            // Byte 1: AVCPacketType (1=AVC NALU)
            // Bytes 2..4: CompositionTime offset (0x00, 0x00, 0x00)
            val p = ByteArray(5 + payload.size)
            p[0] = if (isKeyframe) 0x17.toByte() else 0x27.toByte()
            p[1] = 0x01.toByte()
            p[2] = 0x00.toByte()
            p[3] = 0x00.toByte()
            p[4] = 0x00.toByte()
            System.arraycopy(payload, 0, p, 5, payload.size)
            p
        } else {
            // Enhanced RTMP HEVC video packet (8-byte header for 'hvc1'):
            // Byte 0: IsExHeader (0x80) | (FrameType << 4) | PacketType (1 = CodedFrames)
            // Keyframe: 0x80 | (1 << 4) | 1 = 0x91; Interframe: 0x80 | (2 << 4) | 1 = 0xA1
            // Bytes 1..4: FourCC 'h', 'v', 'c', '1'
            // Bytes 5..7: CompositionTime (0x00, 0x00, 0x00)
            val p = ByteArray(8 + payload.size)
            p[0] = if (isKeyframe) 0x91.toByte() else 0xA1.toByte()
            p[1] = 'h'.code.toByte()
            p[2] = 'v'.code.toByte()
            p[3] = 'c'.code.toByte()
            p[4] = '1'.code.toByte()
            p[5] = 0x00.toByte()
            p[6] = 0x00.toByte()
            p[7] = 0x00.toByte()
            System.arraycopy(payload, 0, p, 8, payload.size)
            p
        }

        val packet = RtmpPacket(
            type = TYPE_VIDEO,
            timestamp = timestampMs,
            payload = flvPayload,
            isKeyframe = isKeyframe
        )
        if (!packetQueue.offer(packet)) {
            if (!isKeyframe) {
                droppedFramesCount.incrementAndGet()
            }
        }
    }

    /**
     * Queues AAC audio packet with proper 2-byte FLV audio tag header.
     */
    fun sendAudio(payload: ByteArray, timestampMs: Long) {
        if (!isPublishing.get()) return

        // RTMP Audio Message requires 2-byte FLV audio tag header for AAC:
        // Byte 0: SoundFormat(10=AAC)<<4 | SoundRate(3=44k)<<2 | SoundSize(1=16bit)<<1 | SoundType(1=Stereo) -> 0xAF
        // Byte 1: AACPacketType (1=AAC raw frame)
        val flvPayload = ByteArray(2 + payload.size)
        flvPayload[0] = 0xAF.toByte()
        flvPayload[1] = 0x01.toByte()
        System.arraycopy(payload, 0, flvPayload, 2, payload.size)

        val packet = RtmpPacket(
            type = TYPE_AUDIO,
            timestamp = timestampMs,
            payload = flvPayload
        )
        packetQueue.offer(packet)
    }

    fun sendMetadata(width: Int, height: Int, fps: Int, videoBitrateKbps: Int, audioBitrateKbps: Int, isHevc: Boolean = false) {
        val out = outStream ?: return
        try {
            val body = ByteArrayOutputStream()
            Amf0.writeString(body, "@setDataFrame")
            Amf0.writeString(body, "onMetaData")

            val meta = mapOf<String, Any?>(
                "duration" to 0.0,
                "width" to width.toDouble(),
                "height" to height.toDouble(),
                "videodatarate" to videoBitrateKbps.toDouble(),
                "framerate" to fps.toDouble(),
                "videocodecid" to if (isHevc) "hvc1" else 7.0,
                "audiodatarate" to audioBitrateKbps.toDouble(),
                "audiosamplerate" to 44100.0,
                "audiosamplesize" to 16.0,
                "stereo" to true,
                "audiocodecid" to 10.0 // AAC
            )
            Amf0.writeEcmaArray(body, meta)

            sendRtmpMessage(
                csid = CSID_COMMAND,
                messageType = TYPE_DATA,
                timestamp = 0,
                streamId = streamId,
                payload = body.toByteArray(),
                out = out
            )
            Log.d(TAG, "Sent RTMP onMetaData (${width}x${height} @ ${fps}fps, HEVC=$isHevc)")
        } catch (e: Exception) {
            Log.e(TAG, "Failed to send metadata", e)
        }
    }

    fun sendHevcSequenceHeader(vps: ByteArray, sps: ByteArray, pps: ByteArray) {
        val out = outStream ?: return
        try {
            val body = ByteArrayOutputStream()
            // Enhanced RTMP SequenceStart: Byte 0 = 0x80 (IsExHeader) | (1 << 4) | 0 = 0x90
            body.write(0x90)
            // FourCC 'hvc1'
            body.write('h'.code)
            body.write('v'.code)
            body.write('c'.code)
            body.write('1'.code)

            // HEVCDecoderConfigurationRecord (HVCC)
            body.write(0x01) // configurationVersion
            body.write(0x01) // general_profile_space (0), general_tier_flag (0), general_profile_idc (1 = Main)
            body.write(0x60)
            body.write(0x00)
            body.write(0x00)
            body.write(0x00)
            body.write(0xB0)
            body.write(0x00)
            body.write(0x00)
            body.write(0x00)
            body.write(0x00)
            body.write(0x00)
            body.write(0x00)
            body.write(0xF0) // general_level_idc
            body.write(0x00) // min_spatial_segmentation_idc
            body.write(0xFC or 0x03) // lengthSizeMinusOne = 3 (4-byte NALU length prefix)
            body.write(if (vps.isNotEmpty()) 0x03 else 0x02) // numOfArrays (VPS, SPS, PPS)

            // Array 1: VPS (type 32 = 0x20)
            if (vps.isNotEmpty()) {
                body.write(0x20 or 0x80)
                body.write(0x00)
                body.write(0x01)
                body.write((vps.size shr 8) and 0xFF)
                body.write(vps.size and 0xFF)
                body.write(vps)
            }

            // Array 2: SPS (type 33 = 0x21)
            body.write(0x21 or 0x80)
            body.write(0x00)
            body.write(0x01)
            body.write((sps.size shr 8) and 0xFF)
            body.write(sps.size and 0xFF)
            body.write(sps)

            // Array 3: PPS (type 34 = 0x22)
            body.write(0x22 or 0x80)
            body.write(0x00)
            body.write(0x01)
            body.write((pps.size shr 8) and 0xFF)
            body.write(pps.size and 0xFF)
            body.write(pps)

            sendRtmpMessage(
                csid = CSID_VIDEO,
                messageType = TYPE_VIDEO,
                timestamp = 0,
                streamId = streamId,
                payload = body.toByteArray(),
                out = out
            )
            Log.d(TAG, "Sent Enhanced RTMP HEVC Sequence Header (VPS ${vps.size}B, SPS ${sps.size}B, PPS ${pps.size}B)")
        } catch (e: Exception) {
            Log.e(TAG, "Failed to send HEVC sequence header", e)
        }
    }

    fun sendAvcSequenceHeader(sps: ByteArray, pps: ByteArray) {
        val out = outStream ?: return
        try {
            val body = ByteArrayOutputStream()
            // Frame type: 1 (Keyframe) + Codec ID: 7 (AVC) -> 0x17
            body.write(0x17)
            // AVC packet type: 0 (AVC sequence header)
            body.write(0x00)
            // Composition time offset: 3 bytes 0
            body.write(0x00)
            body.write(0x00)
            body.write(0x00)

            // AVCDecoderConfigurationRecord
            body.write(0x01) // configurationVersion
            val profile = if (sps.size > 1) sps[1].toInt() and 0xFF else 0x64
            val compat = if (sps.size > 2) sps[2].toInt() and 0xFF else 0x00
            val level = if (sps.size > 3) sps[3].toInt() and 0xFF else 0x1F
            body.write(profile)
            body.write(compat)
            body.write(level)
            body.write(0xFF) // lengthSizeMinusOne: 3 (4 bytes) | 0xFC = 0xFF

            // SPS
            body.write(0xE1) // numOfSequenceParameterSets = 1 | 0xE0
            body.write((sps.size shr 8) and 0xFF)
            body.write(sps.size and 0xFF)
            body.write(sps)

            // PPS
            body.write(0x01) // numOfPictureParameterSets = 1
            body.write((pps.size shr 8) and 0xFF)
            body.write(pps.size and 0xFF)
            body.write(pps)

            sendRtmpMessage(
                csid = CSID_VIDEO,
                messageType = TYPE_VIDEO,
                timestamp = 0,
                streamId = streamId,
                payload = body.toByteArray(),
                out = out
            )
            Log.d(TAG, "Sent AVC Sequence Header: SPS ${sps.size}B, PPS ${pps.size}B")
        } catch (e: Exception) {
            Log.e(TAG, "Failed to send AVC sequence header", e)
        }
    }

    fun sendAacSequenceHeader(ascBytes: ByteArray) {
        val out = outStream ?: return
        try {
            val body = ByteArrayOutputStream()
            // Format: 10 (AAC) + SoundRate: 3 (44kHz) + SoundSize: 1 (16bit) + SoundType: 1 (Stereo) -> 0xAF
            body.write(0xAF)
            // AAC packet type: 0 (AAC sequence header)
            body.write(0x00)
            body.write(ascBytes)

            sendRtmpMessage(
                csid = CSID_AUDIO,
                messageType = TYPE_AUDIO,
                timestamp = 0,
                streamId = streamId,
                payload = body.toByteArray(),
                out = out
            )
            Log.d(TAG, "Sent AAC Sequence Header (${ascBytes.size}B)")
        } catch (e: Exception) {
            Log.e(TAG, "Failed to send AAC sequence header", e)
        }
    }

    private fun doHandshake() {
        val inS = inStream ?: throw IllegalStateException("Input stream is null")
        val outS = outStream ?: throw IllegalStateException("Output stream is null")

        // 1. Client sends C0 (0x03) + C1 (1536 bytes)
        outS.write(0x03)
        val c1 = ByteArray(1536)
        SecureRandom().nextBytes(c1)
        c1[0] = 0; c1[1] = 0; c1[2] = 0; c1[3] = 0
        c1[4] = 0; c1[5] = 0; c1[6] = 0; c1[7] = 0
        outS.write(c1)
        outS.flush()

        // 2. Server sends S0 (1 byte) + S1 (1536 bytes)
        val s0 = inS.read()
        if (s0 != 0x03) {
            throw IllegalStateException("Unexpected S0 version: $s0")
        }
        val s1 = readFully(inS, 1536)

        // 3. Client sends C2 (echoes S1)
        outS.write(s1)
        outS.flush()

        // 4. Server sends S2 (1536 bytes)
        readFully(inS, 1536)
    }

    private fun sendChunkSize(size: Int) {
        val out = outStream ?: return
        val payload = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(size).array()
        sendRtmpMessage(CSID_CONTROL, TYPE_SET_CHUNK_SIZE, 0, 0, payload, out)
    }

    private fun sendConnectCommand(app: String, tcUrl: String) {
        val out = outStream ?: return
        val body = ByteArrayOutputStream()
        Amf0.writeString(body, "connect")
        Amf0.writeNumber(body, 1.0) // Transaction ID
        val obj = mapOf<String, Any?>(
            "app" to app,
            "flashVer" to "FMLE/3.0 (compatible; FMSc/1.0)",
            "tcUrl" to tcUrl,
            "fpad" to false,
            "capabilities" to 15.0,
            "audioCodecs" to 0x0400.toDouble(), // AAC
            "videoCodecs" to 0x0080.toDouble(), // AVC
            "videoFunction" to 1.0
        )
        Amf0.writeObject(body, obj)
        sendRtmpMessage(CSID_COMMAND, TYPE_COMMAND_AMF0, 0, 0, body.toByteArray(), out)
    }

    private fun sendReleaseStream(streamKey: String) {
        val out = outStream ?: return
        val body = ByteArrayOutputStream()
        Amf0.writeString(body, "releaseStream")
        Amf0.writeNumber(body, 2.0)
        Amf0.writeNull(body)
        Amf0.writeString(body, streamKey)
        sendRtmpMessage(CSID_COMMAND, TYPE_COMMAND_AMF0, 0, 0, body.toByteArray(), out)
    }

    private fun sendFCPublish(streamKey: String) {
        val out = outStream ?: return
        val body = ByteArrayOutputStream()
        Amf0.writeString(body, "FCPublish")
        Amf0.writeNumber(body, 3.0)
        Amf0.writeNull(body)
        Amf0.writeString(body, streamKey)
        sendRtmpMessage(CSID_COMMAND, TYPE_COMMAND_AMF0, 0, 0, body.toByteArray(), out)
    }

    private fun sendCreateStream() {
        val out = outStream ?: return
        val body = ByteArrayOutputStream()
        Amf0.writeString(body, "createStream")
        Amf0.writeNumber(body, 4.0)
        Amf0.writeNull(body)
        sendRtmpMessage(CSID_COMMAND, TYPE_COMMAND_AMF0, 0, 0, body.toByteArray(), out)
    }

    private fun sendPublish(streamKey: String) {
        val out = outStream ?: return
        val body = ByteArrayOutputStream()
        Amf0.writeString(body, "publish")
        Amf0.writeNumber(body, 5.0)
        Amf0.writeNull(body)
        Amf0.writeString(body, streamKey)
        Amf0.writeString(body, "live")
        sendRtmpMessage(CSID_COMMAND, TYPE_COMMAND_AMF0, 0, streamId, body.toByteArray(), out)
    }

    data class ServerCommand(
        val name: String,
        val transactionId: Double,
        val propObj: Any?,
        val infoObj: Any?
    )

    /**
     * Reads incoming RTMP chunk stream until an AMF0 command packet is received,
     * correctly handling chunk reassembly, server chunk size changes, and skipping control packets.
     */
    private fun readServerMessagesUntilCommand(targetCommand: String?, timeoutMs: Long): ServerCommand? {
        val inS = inStream ?: return null
        val deadline = System.currentTimeMillis() + timeoutMs

        // Map of csid -> partial message buffer and metadata
        class MessageState(var type: Int = 0, var length: Int = 0, var received: Int = 0, var buffer: ByteArray = ByteArray(0))
        val activeMessages = mutableMapOf<Int, MessageState>()

        while (System.currentTimeMillis() < deadline) {
            val b0 = inS.read()
            if (b0 == -1) break

            val fmt = (b0 shr 6) and 0x03
            var csid = b0 and 0x3F
            if (csid == 0) {
                csid = 64 + inS.read()
            } else if (csid == 1) {
                val b1 = inS.read()
                val b2 = inS.read()
                csid = 64 + b1 + (b2 shl 8)
            }

            val state = activeMessages.getOrPut(csid) { MessageState() }

            when (fmt) {
                0 -> {
                    // 11 bytes: 3 timestamp, 3 len, 1 type, 4 streamId
                    val header = readFully(inS, 11)
                    val len = ((header[3].toInt() and 0xFF) shl 16) or ((header[4].toInt() and 0xFF) shl 8) or (header[5].toInt() and 0xFF)
                    val type = header[6].toInt() and 0xFF
                    state.type = type
                    state.length = len
                    state.received = 0
                    state.buffer = ByteArray(len)
                }
                1 -> {
                    // 7 bytes: 3 delta, 3 len, 1 type
                    val header = readFully(inS, 7)
                    val len = ((header[3].toInt() and 0xFF) shl 16) or ((header[4].toInt() and 0xFF) shl 8) or (header[5].toInt() and 0xFF)
                    val type = header[6].toInt() and 0xFF
                    state.type = type
                    state.length = len
                    state.received = 0
                    state.buffer = ByteArray(len)
                }
                2 -> {
                    // 3 bytes delta, length and type unchanged
                    readFully(inS, 3)
                    state.received = 0
                    state.buffer = ByteArray(state.length)
                }
                3 -> {
                    // Continuation chunk: format remains same
                }
            }

            val toRead = minOf(serverChunkSize, state.length - state.received)
            if (toRead > 0) {
                val chunk = readFully(inS, toRead)
                System.arraycopy(chunk, 0, state.buffer, state.received, toRead)
                state.received += toRead
            }

            // If message is complete
            if (state.received >= state.length) {
                when (state.type) {
                    TYPE_SET_CHUNK_SIZE -> {
                        if (state.buffer.size >= 4) {
                            val newSize = ByteBuffer.wrap(state.buffer).order(ByteOrder.BIG_ENDIAN).int
                            if (newSize > 0) {
                                serverChunkSize = newSize
                                Log.d(TAG, "Server updated chunk size: $serverChunkSize")
                            }
                        }
                    }
                    TYPE_WINDOW_ACK_SIZE, TYPE_SET_PEER_BANDWIDTH, TYPE_USER_CONTROL -> {
                        // Handled silently
                    }
                    TYPE_COMMAND_AMF0 -> {
                        try {
                            val bais = ByteArrayInputStream(state.buffer)
                            val cmdName = Amf0.readValue(bais)?.toString() ?: ""
                            val transId = (Amf0.readValue(bais) as? Number)?.toDouble() ?: 0.0
                            val propObj = Amf0.readValue(bais)
                            val infoObj = Amf0.readValue(bais)

                            val cmd = ServerCommand(cmdName, transId, propObj, infoObj)
                            if (targetCommand == null || cmdName == targetCommand || cmdName == "_error") {
                                return cmd
                            }
                        } catch (e: Exception) {
                            Log.w(TAG, "Failed parsing AMF0 command: ${e.message}")
                        }
                    }
                }
            }
        }
        return null
    }

    private fun sendRtmpMessage(
        csid: Int,
        messageType: Int,
        timestamp: Long,
        streamId: Int,
        payload: ByteArray,
        out: OutputStream
    ) {
        synchronized(writeLock) {
            val length = payload.size
            var offset = 0
            var isFirstChunk = true

            while (offset < length) {
                val chunkSize = minOf(CHUNK_SIZE, length - offset)
                if (isFirstChunk) {
                    // Type 0 Chunk Header: 1 byte basic + 11 bytes message header
                    val basicHeader = (0 shl 6) or (csid and 0x3F)
                    out.write(basicHeader)

                    // Timestamp (3 bytes)
                    val ts = if (timestamp >= 0xFFFFFF) 0xFFFFFFL else timestamp
                    out.write(((ts shr 16) and 0xFF).toInt())
                    out.write(((ts shr 8) and 0xFF).toInt())
                    out.write((ts and 0xFF).toInt())

                    // Message length (3 bytes)
                    out.write(((length shr 16) and 0xFF))
                    out.write(((length shr 8) and 0xFF))
                    out.write((length and 0xFF))

                    // Message type (1 byte)
                    out.write(messageType)

                    // Stream ID (4 bytes little endian)
                    out.write(streamId and 0xFF)
                    out.write((streamId shr 8) and 0xFF)
                    out.write((streamId shr 16) and 0xFF)
                    out.write((streamId shr 24) and 0xFF)

                    // Extended timestamp if >= 0xFFFFFF
                    if (timestamp >= 0xFFFFFF) {
                        val ext = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(timestamp.toInt()).array()
                        out.write(ext)
                    }
                    isFirstChunk = false
                } else {
                    // Type 3 Chunk Header: 1 byte basic header
                    val basicHeader = (3 shl 6) or (csid and 0x3F)
                    out.write(basicHeader)
                    if (timestamp >= 0xFFFFFF) {
                        val ext = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(timestamp.toInt()).array()
                        out.write(ext)
                    }
                }

                out.write(payload, offset, chunkSize)
                offset += chunkSize
            }
            out.flush()
        }
    }

    fun disconnect() {
        isPublishing.set(false)
        isConnected.set(false)
        senderThread?.interrupt()
        senderThread = null
        packetQueue.clear()

        try {
            inStream?.close()
        } catch (_: Exception) {}
        try {
            outStream?.close()
        } catch (_: Exception) {}
        try {
            socket?.close()
        } catch (_: Exception) {}

        socket = null
        inStream = null
        outStream = null
    }

    private fun readFully(inS: InputStream, length: Int): ByteArray {
        val buf = ByteArray(length)
        var total = 0
        while (total < length) {
            val count = inS.read(buf, total, length - total)
            if (count == -1) throw IllegalStateException("EOF reached prematurely after $total of $length bytes")
            total += count
        }
        return buf
    }

    data class TargetAddress(
        val host: String,
        val port: Int,
        val app: String,
        val streamKey: String,
        val tcUrl: String,
        val isSsl: Boolean
    )

    private fun parseUrl(serverUrl: String, customStreamKey: String): TargetAddress {
        var cleanUrl = serverUrl.trim()
        val isSsl = cleanUrl.startsWith("rtmps://", ignoreCase = true)
        val defaultPort = if (isSsl) DEFAULT_RTMPS_PORT else DEFAULT_RTMP_PORT

        if (!cleanUrl.startsWith("rtmp://", ignoreCase = true) && !cleanUrl.startsWith("rtmps://", ignoreCase = true)) {
            cleanUrl = "rtmp://$cleanUrl"
        }

        val uri = try {
            URI(cleanUrl)
        } catch (e: Exception) {
            URI("rtmp://a.rtmp.youtube.com/live2")
        }

        val host = uri.host ?: "127.0.0.1"
        val port = if (uri.port != -1) uri.port else defaultPort
        val path = uri.path?.trimStart('/') ?: "live2"

        val parts = path.split('/')
        val app = if (parts.isNotEmpty() && parts[0].isNotBlank()) parts[0] else "live2"
        val keyInUrl = if (parts.size > 1) parts.drop(1).joinToString("/") else ""

        val finalKey = if (customStreamKey.isNotBlank()) customStreamKey.trim() else keyInUrl
        
        // Standard RTMP servers (e.g. YouTube, Twitch) require tcUrl without port if default port
        val tcUrl = if (port == defaultPort) {
            "${if (isSsl) "rtmps" else "rtmp"}://$host/$app"
        } else {
            "${if (isSsl) "rtmps" else "rtmp"}://$host:$port/$app"
        }

        return TargetAddress(
            host = host,
            port = port,
            app = app,
            streamKey = finalKey,
            tcUrl = tcUrl,
            isSsl = isSsl
        )
    }

    private fun createAndConnectSocket(target: TargetAddress, timeoutMs: Int): Socket {
        val allAddresses = try {
            val addrs = java.net.InetAddress.getAllByName(target.host)
            addrs.sortedBy { if (it is java.net.Inet4Address) 0 else 1 }
        } catch (_: Exception) {
            listOf(java.net.InetAddress.getByName(target.host))
        }

        var lastEx: Exception? = null
        for (addr in allAddresses) {
            try {
                if (target.isSsl) {
                    val sslFactory = SSLSocketFactory.getDefault() as SSLSocketFactory
                    val ssl = sslFactory.createSocket() as SSLSocket
                    try {
                        val params = ssl.sslParameters
                        params.serverNames = listOf(javax.net.ssl.SNIHostName(target.host))
                        ssl.sslParameters = params
                    } catch (_: Exception) {}
                    ssl.tcpNoDelay = true
                    ssl.sendBufferSize = 256 * 1024
                    ssl.receiveBufferSize = 64 * 1024
                    ssl.connect(InetSocketAddress(addr, target.port), timeoutMs)
                    ssl.startHandshake()
                    return ssl
                } else {
                    val plain = Socket()
                    plain.tcpNoDelay = true
                    plain.sendBufferSize = 256 * 1024
                    plain.receiveBufferSize = 64 * 1024
                    plain.connect(InetSocketAddress(addr, target.port), timeoutMs)
                    return plain
                }
            } catch (e: Exception) {
                lastEx = e
                Log.w(TAG, "Failed connecting to ${addr.hostAddress}:${target.port}: ${e.message}")
            }
        }
        throw (lastEx ?: java.io.IOException("Unable to connect to ${target.host}:${target.port}"))
    }

    /**
     * Diagnostic connection test: verifies TCP/TLS connection, handshake, and handshake latency.
     */
    fun testConnection(serverUrl: String, streamKey: String): Pair<Boolean, String> {
        val startTime = System.currentTimeMillis()
        try {
            val target = parseUrl(serverUrl, streamKey)
            val testSocket = createAndConnectSocket(target, 5000)

            val testIn = BufferedInputStream(testSocket.getInputStream())
            val testOut = BufferedOutputStream(testSocket.getOutputStream())

            // C0 + C1
            testOut.write(0x03)
            val c1 = ByteArray(1536)
            testOut.write(c1)
            testOut.flush()

            val s0 = testIn.read()
            if (s0 != 0x03) {
                testSocket.close()
                return Pair(false, "Server rejected RTMP handshake (S0: $s0)")
            }
            val s1 = readFully(testIn, 1536)
            testOut.write(s1)
            testOut.flush()
            readFully(testIn, 1536)

            val latency = System.currentTimeMillis() - startTime
            testSocket.close()
            return Pair(true, "Connected successfully! Handshake verified (${target.host}:${target.port}, ${latency}ms latency)")
        } catch (e: Exception) {
            return Pair(false, "Connection test failed: ${e.localizedMessage ?: e.message}")
        }
    }
}
