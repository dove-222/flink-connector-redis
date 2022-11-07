package org.apache.flink.streaming.connectors.redis.config;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author guozixuan
 * cluster 模式
 */
public class RedisClusterFlinkConfig extends FlinkConfigBase implements Serializable {

    private static final long serialVersionUID = 1L;

    public RedisClusterFlinkConfig(String host, String password, int port, int database, int connectTimeout, boolean isAsync) {
        super(host, password, port, database, connectTimeout, isAsync);
    }

    public static class Builder {
        private String host;
        private String password;
        private int port;
        private int database;
        private int connectTimeout;
        private boolean isAsync;

        public RedisClusterFlinkConfig build() {
            return new RedisClusterFlinkConfig(host, password, port, database, connectTimeout, isAsync);
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

        public Builder setAsync(boolean async) {
            isAsync = async;
            return this;
        }
    }

    @Override
    public String toString() {
        return "RedisClusterFlinkConfig{" +
                "host='" + host + '\'' +
                ", password='" + password + '\'' +
                ", port=" + port +
                ", database=" + database +
                ", connectTimeout=" + connectTimeout +
                '}';
    }
}
