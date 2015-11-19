package org.graylog.inputs.beats.server;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.DefaultChannelPipeline;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.ssl.SslHandler;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.concurrent.Executors;

/**
 * Created on 7/4/15.
 */
public class LumberjackServer {
    private ServerBootstrap bootstrap;
    private EventListener eventListener;
    private ServerConfiguration configuration;
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(LumberjackServer.class);

    public LumberjackServer(ServerConfiguration configuration,
                            EventListener eventListener) {
        this.configuration = configuration;
        this.eventListener = eventListener;
    }

    public void start() {
        bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
                Executors.newFixedThreadPool(1),
                Executors.newCachedThreadPool()
        ));

        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = new DefaultChannelPipeline();
                if(configuration.isSslEnabled()) {
                    pipeline.addLast("ssl", new SslHandler(getSSLEngine()));
                }
                pipeline.addLast("decoder", new LumberjackDecoder());
                pipeline.addLast("logHandler", new EventHandler(eventListener));
                return pipeline;
            }
        });
        bootstrap.bind(new InetSocketAddress(configuration.getIpAddress(), configuration.getPort()));
    }

    private SSLEngine getSSLEngine() throws GeneralSecurityException, IOException {
        SSLContext context;
        char[] storepass = configuration.getKeyStorePass().toCharArray();
        char[] keypass = configuration.getKeyPass().toCharArray();
        String storePath = configuration.getKeyStorePath();

        try {
            context = SSLContext.getInstance("TLS");
            KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
            FileInputStream fin = new FileInputStream(storePath);
            KeyStore ks = KeyStore.getInstance("JKS");
            ks.load(fin, storepass);

            kmf.init(ks, keypass);
            context.init(kmf.getKeyManagers(), null, null);
        } catch (GeneralSecurityException | IOException e) {
            //e.printStackTrace();
            LOGGER.warn("Exception while creating channel pipeline",e);
            throw e;
        }
        SSLEngine engine = context.createSSLEngine();
        engine.setUseClientMode(false);
        return engine;
    }

    public void stop() {
        bootstrap.shutdown();
    }
}
