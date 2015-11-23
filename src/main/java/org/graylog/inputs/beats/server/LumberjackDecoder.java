package org.graylog.inputs.beats.server;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.zip.InflaterInputStream;

/**
 * Created on 7/4/15.
 */
public class LumberjackDecoder extends FrameDecoder {

    private long nextAckSeqNum;
    private long sequenceNum,prevSequenceNum;
    private static final Logger LOGGER = LoggerFactory.getLogger(LumberjackDecoder.class);
    private ObjectMapper objectMapper = new ObjectMapper();

    private final byte FRAME_WINDOWSIZE = 0x57, FRAME_DATA = 0x44, FRAME_COMPRESSED = 0x43, FRAME_ACK = 0x41,
            FRAME_JSON = 0x4A;

    public LumberjackDecoder() {
    }

    @Override
    protected Object decode(ChannelHandlerContext channelHandlerContext, Channel channel, ChannelBuffer channelBuffer) throws Exception {

        return processBuffer(channel, channelBuffer);
    }


    private List<Event> processBuffer(Channel channel, ChannelBuffer channelBuffer) throws IOException {

        channelBuffer.markReaderIndex();
        byte version = channelBuffer.readByte();
        byte frameType = channelBuffer.readByte();
        List<Event> events = null;

        switch (frameType) {
            case FRAME_WINDOWSIZE: //'W'
                processWindowSizeFrame(channelBuffer);
                break;
            case FRAME_DATA: //'D'
                events = Collections.singletonList(processDataFrame(channelBuffer));

                //Handle sequence number roll-over. Send ack for prev seq num and start from 0
                if(sequenceNum < prevSequenceNum)
                {
                    sendAck(channel,prevSequenceNum);
                    nextAckSeqNum = 0;
                }
                //send ack
                else if (sequenceNum == nextAckSeqNum) {
                    sendAck(channel,sequenceNum);
                }
                break;
            case FRAME_COMPRESSED: //'C'
                events = processCompressedFrame(channel, channelBuffer);
                break;
            case FRAME_JSON:
                events = Arrays.asList(processJsonFrame(channel,channelBuffer));
                break;
        }
        return events;
    }

    private Event processJsonFrame(Channel channel, ChannelBuffer channelBuffer) throws IOException {
        sequenceNum = channelBuffer.readUnsignedInt();
        int jsonLength = (int) channelBuffer.readUnsignedInt();
        byte[] data = new byte[jsonLength];
        channelBuffer.readBytes(data);
        if(LOGGER.isDebugEnabled()) {
            LOGGER.debug(new String(data));
        }
        Map<String, Object> map = objectMapper.readValue(data,new TypeReference<Map<String, Object>>(){});
        return new Event(map);
    }

    private List<Event> processCompressedFrame(Channel channel, ChannelBuffer channelBuffer) throws IOException {
        if (channelBuffer.readableBytes() >= 4) {
            long payloadLength = channelBuffer.readUnsignedInt();
            if (channelBuffer.readableBytes() < payloadLength) {
                channelBuffer.resetReaderIndex();
            } else {
                byte[] data = new byte[(int) payloadLength];
                channelBuffer.readBytes(data);

                if (data.length == payloadLength) {
                    InputStream in =
                            new InflaterInputStream(new ByteArrayInputStream(data));
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    byte[] buffer = new byte[1024];
                    int len;
                    while ((len = in.read(buffer)) > 0) {
                        out.write(buffer, 0, len);
                    }
                    in.close();
                    out.close();
                    data = out.toByteArray();
                    return processCompressedDataFrames(channel, ChannelBuffers.copiedBuffer(data));
                }
            }
        } else {
            channelBuffer.resetReaderIndex();
        }
        return null;
    }

    private List<Event> processCompressedDataFrames(Channel channel, ChannelBuffer channelBuffer) throws IOException {
        List<Event> events = new LinkedList<>();
        while (channelBuffer.readable()) {
            events.addAll(processBuffer(channel, channelBuffer));
        }
        return events;
    }

    private void processWindowSizeFrame(ChannelBuffer channelBuffer) {
        if (channelBuffer.readableBytes() < 4) {
            channelBuffer.resetReaderIndex();
        } else {
            long windowSize = channelBuffer.readUnsignedInt();
            nextAckSeqNum = sequenceNum + windowSize;
            if(LOGGER.isDebugEnabled()) {
                LOGGER.debug("Window size ->" + windowSize + " next ack seq num " + nextAckSeqNum);
            }
        }
    }

    private Event processDataFrame(ChannelBuffer channelBuffer) {
        prevSequenceNum = sequenceNum;
        sequenceNum = channelBuffer.readUnsignedInt();
        if(LOGGER.isDebugEnabled()) {
            LOGGER.debug("Sequence number ->" + sequenceNum);
        }
        long pairCount = channelBuffer.readUnsignedInt();
        Map<String, Object> logDataMap = new HashMap<>((int) pairCount);
        for (int i = 0; i < pairCount; i++) {
            long keyLength = channelBuffer.readUnsignedInt();
            byte[] bytes = new byte[(int) keyLength];
            channelBuffer.readBytes(bytes);
            String key = new String(bytes);

            long valueLength = channelBuffer.readUnsignedInt();
            bytes = new byte[(int) valueLength];
            channelBuffer.readBytes(bytes);
            String value = new String(bytes);

            logDataMap.put(key, value);
        }

        return createLogEvent(logDataMap);
    }

    private Event createLogEvent(Map<String, Object> logDataMap) {
        return new Event(logDataMap);
    }

    private void sendAck(final Channel channel, long seqNum) throws IOException {
        if(LOGGER.isDebugEnabled()) {
            LOGGER.debug("Sending Ack for " + seqNum);
        }
        ChannelBuffer buffer = ChannelBuffers.buffer(6);
        buffer.writeBytes(new byte[]{0x31, FRAME_ACK});
        buffer.writeInt((int) seqNum);
        ChannelFuture future = channel.write(buffer);
        future.awaitUninterruptibly();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        super.exceptionCaught(ctx, e);
        LOGGER.warn("Exception while process channel. So closing the channel " + ctx.getChannel(), e.getCause());
        e.getChannel().close();
    }
}
