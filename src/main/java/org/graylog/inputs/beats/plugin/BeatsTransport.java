package org.graylog.inputs.beats.plugin;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import org.graylog.inputs.beats.server.Event;
import org.graylog.inputs.beats.server.EventListener;
import org.graylog.inputs.beats.server.LumberjackServer;
import org.graylog.inputs.beats.server.ServerConfiguration;
import org.graylog2.plugin.ServerStatus;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.configuration.fields.ConfigurationField;
import org.graylog2.plugin.configuration.fields.NumberField;
import org.graylog2.plugin.configuration.fields.TextField;
import org.graylog2.plugin.inputs.MessageInput;
import org.graylog2.plugin.inputs.MisfireException;
import org.graylog2.plugin.inputs.annotations.ConfigClass;
import org.graylog2.plugin.inputs.annotations.FactoryClass;
import org.graylog2.plugin.inputs.codecs.CodecAggregator;
import org.graylog2.plugin.inputs.transports.Transport;
import org.graylog2.plugin.journal.RawMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created on 9/4/15.
 */
public class BeatsTransport implements Transport {

    private static final Logger LOGGER = LoggerFactory.getLogger(BeatsTransport.class.getName());
    private final Configuration configuration;
    private final MetricRegistry metricRegistry;
    private ServerStatus serverStatus;
    private LumberjackServer lumberjackServer;
    private static final String CK_KEYSTORE_PATH = "keystorePath";
    private static final String CK_KEYSTORE_PASSWORD = "keystorePassword";
    private static final String CK_KEY_PASSWORD = "keyPassword";
    private static final String CK_BIND_IP = "bindIP";
    private static final String CK_BIND_PORT = "bindPort";

    @AssistedInject
    public BeatsTransport(@Assisted Configuration configuration,
                          MetricRegistry metricRegistry,
                          ServerStatus serverStatus) {
        this.configuration = configuration;
        this.metricRegistry = metricRegistry;
        this.serverStatus = serverStatus;
    }

    @Override
    public void setMessageAggregator(CodecAggregator codecAggregator) {

    }

    @Override
    public void launch(final MessageInput messageInput) throws MisfireException {
        EventListener listener = new EventListener() {
            @Override
            public void onEvents(List<Event> list) {
                ObjectMapper mapper = new ObjectMapper();
                try {
                    for (Event event : list) {
                        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
                        mapper.writeValue(byteStream,convertToGELF(event));
                        messageInput.processRawMessage(new RawMessage(byteStream.toByteArray()));
                        byteStream.close();
                    }
                } catch (Exception e) {
                    LOGGER.warn("Exception while processing event ",e);
                }
            }
        };
        ServerConfiguration serverConfiguration = new ServerConfiguration();
        serverConfiguration.setIpAddress(configuration.getString(CK_BIND_IP));
        serverConfiguration.setPort(configuration.getInt(CK_BIND_PORT));
        serverConfiguration.setKeyStorePath(configuration.getString(CK_KEYSTORE_PATH));
        serverConfiguration.setKeyStorePass(configuration.getString(CK_KEYSTORE_PASSWORD));
        serverConfiguration.setKeyPass(configuration.getString(CK_KEY_PASSWORD));
        LOGGER.info("Starting BeatsTransport with config :" + configuration);
        lumberjackServer = new LumberjackServer(serverConfiguration,
                listener);
        lumberjackServer.start();
        LOGGER.info("BeatsTransport started");
    }

    private Map<String,Object> convertToGELF(Event lumberjackEvent) {
        Map<String,Object> gelfMessage = null;
        HashMap<String,String> metaData = (HashMap<String,String>)lumberjackEvent.getEventData().get("@metadata");
        String beat = metaData.get("beat");
        if("filebeat".equals(beat)) {
            gelfMessage = parseFileBeat(lumberjackEvent);
        } else if("topbeat".equals(beat)) {

        }
        return gelfMessage;
    }

    /**
     * Sample File Beat JSON
     * {
     "@metadata":{
     "beat":"filebeat",
     "type":"log"
     },
     "@timestamp":"2015-11-17T19:17:12.966Z",
     "count":1,
     "fields":null,
     "fileinfo":{

     },
     "input_type":"log",
     "line":7,
     "message":"Sep 28 18:31:05 vagrant-ubuntu-trusty-64 sshd[1347]: pam_unix(sshd:session): session opened for user vagrant by (uid=0)",
     "offset":603,
     "shipper":"vagrant-ubuntu-trusty-64",
     "source":"/var/log/auth.log",
     "type":"log"
     }
     *
     */
    private Map<String,Object> parseFileBeat(Event event) {
        Map<String,Object> gelfMessage = new LinkedHashMap<>();
        gelfMessage.put("version", "1.1");
        gelfMessage.put("host",event.getEventData().remove("shipper"));
        gelfMessage.put("file",event.getEventData().remove("source"));
        gelfMessage.put("short_message", event.getEventData().remove("message"));
        Map<String,Object> fields = (Map<String,Object>)event.getEventData().get("fields");
        if(fields != null) {
            gelfMessage.putAll(fields);
        }
        return gelfMessage;
    }



    @Override
    public void stop() {
        lumberjackServer.stop();
    }

    @Override
    public MetricSet getMetricSet() {
        return null;
    }

    @FactoryClass
    public interface Factory extends Transport.Factory<BeatsTransport> {
        @Override
        BeatsTransport create(Configuration configuration);

        @Override
        Config getConfig();
    }

    @ConfigClass
    public static class Config implements Transport.Config {
        @Override
        public ConfigurationRequest getRequestedConfiguration() {
            final ConfigurationRequest cr = new ConfigurationRequest();
            cr.addField(new TextField(CK_KEYSTORE_PATH,
                    "Keystore Path",
                    "",
                    "Absolute path of JKS keystore"));
            cr.addField(new TextField(CK_KEYSTORE_PASSWORD,
                    "Keystore Password",
                    "",
                    "-deststorepass argument in keytool",
                    ConfigurationField.Optional.NOT_OPTIONAL,
                    TextField.Attribute.IS_PASSWORD));
            cr.addField(new TextField(CK_KEY_PASSWORD,
                    "Key Password",
                    "",
                    "-destkeypass argument in keytool",
                    ConfigurationField.Optional.NOT_OPTIONAL,
                    TextField.Attribute.IS_PASSWORD));
            cr.addField(new TextField(CK_BIND_IP,
                    "Bind IP Address",
                    "0.0.0.0",
                    "Local IP Address to bind",
                    ConfigurationField.Optional.NOT_OPTIONAL));
            cr.addField(new NumberField(CK_BIND_PORT,
                    "Port",
                    5043,
                    "Local port to listen for events",
                    ConfigurationField.Optional.NOT_OPTIONAL,
                    NumberField.Attribute.IS_PORT_NUMBER));
            return cr;
        }
    }
}
