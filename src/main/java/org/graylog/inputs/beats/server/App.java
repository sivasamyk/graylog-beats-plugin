package org.graylog.inputs.beats.server;

import java.util.List;

/**
 * Hello world!
 *
 */
public class App 
{
    static int count;
    public static void main( String[] args ) throws Exception
    {
        EventListener eventListener = new EventListener() {
            public void onEvents(List<Event> logEvents) {
                for(Event event : logEvents) {
                    System.out.println(event.getEventData());
                    System.out.println(++count);
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
