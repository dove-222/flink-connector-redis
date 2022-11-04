package org.apache.flink.streaming.connectors.redis.config;

import java.io.Serializable;

/**
 * @author guozixuan
 * redis single mode config
 */
public class RedisSingleFlinkConfig extends FlinkConfigBase implements Serializable {

    private static final long serialVersionUID = 1L;

    public RedisSingleFlinkConfig(String host, String password, int port, int database, int connectTimeout) {
        super(host, password, port, database, connectTimeout);
    }

    public static class Builder {
        private String host;
        private String password;
        private int port;
        private int database;
        private int connectTimeout;

        public RedisSingleFlinkConfig build() {
            return new RedisSingleFlinkConfig(host, password, port, database, connectTimeout);
        }

        public Builder setHost(String host) {
            this.host = host;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setDatabase(int database) {
            this.database = database;
            return this;
        }

        public Builder setConnectTimeout(int connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }
    }

    @Override
    public String toString() {
        return "RedisSingleFlinkConfig{" +
                "host='" + host + '\'' +
                ", password='" + password + '\'' +
                ", port=" + port +
                ", database=" + database +
                ", connectTimeout=" + connectTimeout +
                '}';
    }
}
