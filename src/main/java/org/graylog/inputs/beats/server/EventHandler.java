package org.graylog.inputs.beats.server;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

import java.util.List;

/**
 * Created on 9/4/15.
 */
public class EventHandler extends SimpleChannelHandler {

    private EventListener eventListener;

    public EventHandler(EventListener eventListener) {
        this.eventListener = eventListener;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        super.messageReceived(ctx, e);
        Object message = e.getMessage();

        if(message != null)
        {
            List<Event> events = (List<Event>)message;
            eventListener.onEvents(events);
        }
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        super.channelClosed(ctx, e);
    }
}
