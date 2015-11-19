package org.graylog.inputs.beats.server;

/**
 * Created on 11/4/15.
 */
public class ServerConfiguration {
    private String keyStorePath, keyStorePass, keyPass, ipAddress;
    private int port;

    public String getKeyStorePath() {
        return keyStorePath;
    }

    public void setKeyStorePath(String keyStorePath) {
        this.keyStorePath = keyStorePath;
    }

    public String getKeyStorePass() {
        return keyStorePass;
    }

    public void setKeyStorePass(String keyStorePass) {
        this.keyStorePass = keyStorePass;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public String toString() {
        return "ServerConfiguration{" +
                "keyStorePath='" + keyStorePath + '\'' +
                ", ipAddress='" + ipAddress + '\'' +
                ", port=" + port +
                '}';
    }

    public String getKeyPass() {
        return keyPass;
    }

    public void setKeyPass(String keyPass) {
        this.keyPass = keyPass;
    }

    public boolean isSslEnabled() {
        return keyStorePath != null;
    }
}
