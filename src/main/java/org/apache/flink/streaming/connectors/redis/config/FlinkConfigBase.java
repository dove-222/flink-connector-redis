package org.apache.flink.streaming.connectors.redis.config;

import java.io.Serializable;

public abstract class FlinkConfigBase implements Serializable {

    private static final long serialVersionUID = 1L;

    protected final int connectTimeout;

    protected final String password;

    public FlinkConfigBase(int connectionTimeout, String password) {
        this.connectTimeout = connectionTimeout;
        this.password = password;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public String getPassword() {
        return password;
    }

    @Override
    public String toString() {
        return "FlinkConfigBase{" +
                "connectionTimeout=" + connectTimeout +
                ", password='" + password + '\'' +
                '}';
    }
}
