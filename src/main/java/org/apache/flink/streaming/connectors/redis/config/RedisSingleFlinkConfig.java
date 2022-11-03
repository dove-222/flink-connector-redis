package org.apache.flink.streaming.connectors.redis.config;

import java.io.Serializable;

/**
 * @author guozixuan
 * redis single mode config
 */
public class RedisSingleFlinkConfig extends FlinkConfigBase implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String host;
    private final int port;
    private final int database;

    public RedisSingleFlinkConfig(int connectionTimeout, String password,
                                  String host, int port, int database) {
        super(connectionTimeout, password);
        this.host = host;
        this.port = port;
        this.database = database;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getDatabase() {
        return database;
    }

    public static class Builder {
        private String host;
        private int port;
        private int connectionTimeout;
        private int database;
        private String password;

        public RedisSingleFlinkConfig build() {
            return new RedisSingleFlinkConfig(connectionTimeout, password, host, port, database);
        }

        public Builder setHost(String host) {
            this.host = host;
            return this;
        }

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setConnectionTimeout(int connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        public Builder setDatabase(int database) {
            this.database = database;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }
    }
}
