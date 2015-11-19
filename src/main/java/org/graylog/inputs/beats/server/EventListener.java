package org.graylog.inputs.beats.server;

import java.util.List;

/**
 * Created on 9/4/15.
 */
public interface EventListener {
    void onEvents(List<Event> events);
}
