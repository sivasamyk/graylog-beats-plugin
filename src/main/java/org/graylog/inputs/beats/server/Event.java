package org.graylog.inputs.beats.server;

import java.util.Map;

/**
 * Created on 9/4/15.
 */
public class Event {

    private Map<String,Object> eventData;

    public Event(Map<String, Object> eventData) {
        this.eventData = eventData;
    }

    public Map<String, Object> getEventData() {
        return eventData;
    }
}
