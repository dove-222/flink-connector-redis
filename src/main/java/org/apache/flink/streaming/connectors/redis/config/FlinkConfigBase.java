package org.apache.flink.streaming.connectors.redis.config;

import java.io.Serializable;

/**
 * @author guozixuan
 */
public abstract class FlinkConfigBase implements Serializable {

    private static final long serialVersionUID = 1L;

    protected final String host;

    protected final String password;

    protected final int port;

    protected final int database;

    protected final int connectTimeout;

    public FlinkConfigBase(String host, String password, int port, int database, int connectTimeout) {
        this.host = host;
        this.password = password;
        this.port = port;
        this.database = database;
        this.connectTimeout = connectTimeout;
    }

    public String getHost() {
        return host;
    }

    public String getPassword() {
        return password;
    }

    public int getPort() {
        return port;
    }

    public int getDatabase() {
        return database;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    @Override
    public String toString() {
        return "FlinkConfigBase{" +
                "connectionTimeout=" + connectTimeout +
                ", password='" + password + '\'' +
                '}';
    }
}
