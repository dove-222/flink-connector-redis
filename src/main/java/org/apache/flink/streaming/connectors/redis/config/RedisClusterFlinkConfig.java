package org.apache.flink.streaming.connectors.redis.config;

import java.io.Serializable;
import java.util.Objects;

/**
 * cluster 模式
 */
public class RedisClusterFlinkConfig extends FlinkConfigBase implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String nodesInfo;

    public RedisClusterFlinkConfig(int connectionTimeout, String password, String nodesInfo) {
        super(connectionTimeout, password);

        Objects.requireNonNull(nodesInfo, "nodesInfo information should be presented");
        this.nodesInfo = nodesInfo;
    }

    /** Builder for initializing {@link RedisClusterFlinkConfig}. */
    public static class Builder {
        private String nodesInfo;
        private int timeout;
        private String password;

        public Builder setNodesInfo(String nodesInfo) {
            this.nodesInfo = nodesInfo;
            return this;
        }

        /**
         * Sets socket / connection timeout.
         *
         * @param timeout socket / connection timeout, default value is 2000
         * @return Builder itself
         */
        public Builder setTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        /**
         * Builds ClusterConfig.
         *
         * @return ClusterConfig
         */
        public RedisClusterFlinkConfig build() {
            return new RedisClusterFlinkConfig(timeout, password, nodesInfo);
        }
    }

    public String getNodesInfo() {
        return nodesInfo;
    }

    @Override
    public String toString() {
        return "RedisClusterFlinkConfig{" +
                "connectionTimeout=" + connectTimeout +
                ", password='" + password + '\'' +
                '}';
    }
}
