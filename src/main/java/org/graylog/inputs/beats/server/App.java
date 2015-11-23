package org.graylog.inputs.beats.server;

import java.util.List;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        EventListener eventListener = new EventListener() {
            public void onEvents(List<Event> logEvents) {
                for(Event event : logEvents) {
                    System.out.println(event.getEventData());
                }
            }
        };
        ServerConfiguration configuration = new ServerConfiguration();
        configuration.setIpAddress("0.0.0.0");
        configuration.setPort(5044);
        configuration.setKeyStorePass("pass");
        configuration.setKeyPass("pass");
      //  configuration.setKeyStorePath("/path/to/store.jks");
        new LumberjackServer(configuration, eventListener).start();
    }
}
