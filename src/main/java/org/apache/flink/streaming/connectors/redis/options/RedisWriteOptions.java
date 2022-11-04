package org.apache.flink.streaming.connectors.redis.options;


/**
 * @author guozixuan
 * redis sink options
 */
public class RedisWriteOptions {

    protected final String host;

    protected final int port;

    private final String password;

    private final int database;

    private final int sinkTtl;

    private final int connectTimeout;

    private final String dataType;

    private final long bufferFlushMaxSizeInBytes;
    private final long bufferFlushMaxMutations;
    private final long bufferFlushIntervalMillis;
    private final Integer parallelism;

    public RedisWriteOptions(String host, int port, String password,
                             int database, int sinkTtl, int connectTimeout,
                             String dataType, long bufferFlushMaxSizeInBytes,
                             long bufferFlushMaxMutations, long bufferFlushIntervalMillis,
                             Integer parallelism) {
        this.host = host;
        this.port = port;
        this.password = password;
        this.database = database;
        this.sinkTtl = sinkTtl;
        this.connectTimeout = connectTimeout;
        this.dataType = dataType;
        this.bufferFlushMaxSizeInBytes = bufferFlushMaxSizeInBytes;
        this.bufferFlushMaxMutations = bufferFlushMaxMutations;
        this.bufferFlushIntervalMillis = bufferFlushIntervalMillis;
        this.parallelism = parallelism;
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

    public int getSinkTtl() {
        return sinkTtl;
    }

    public String getDataType() {
        return dataType;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public long getBufferFlushMaxSizeInBytes() {
        return bufferFlushMaxSizeInBytes;
    }

    public long getBufferFlushMaxMutations() {
        return bufferFlushMaxMutations;
    }

    public long getBufferFlushIntervalMillis() {
        return bufferFlushIntervalMillis;
    }

    public Integer getParallelism() {
        return parallelism;
    }

    public int getDatabase() {
        return database;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder of {@link RedisWriteOptions}. */
    public static class Builder {

        protected String host;

        protected int port;

        private String password;

        private int database;

        private int sinkTtl;

        private String dataType;

        private int connectTimeout;

        private long bufferFlushMaxSizeInBytes;
        private long bufferFlushMaxMutations;
        private long bufferFlushIntervalMillis;
        private Integer parallelism;

        /**
         * optional, lookup cache max size, over this value, the old data will be eliminated.
         */
        public Builder setHost(String host) {
            this.host = host;
            return this;
        }

        /**
         * optional, lookup cache expire mills, over this time, the old data will expire.
         */
        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder setDatabase(int database) {
            this.database = database;
            return this;
        }

        /** optional, max retry times for Redis connector. */
        public Builder setSinkTtl(int sinkTtl) {
            this.sinkTtl = sinkTtl;
            return this;
        }

        public Builder setDataType(String dataType) {
            this.dataType = dataType;
            return this;
        }

        public Builder setConnectTimeout(int connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public Builder setBufferFlushMaxSizeInBytes(long bufferFlushMaxSizeInBytes) {
            this.bufferFlushMaxSizeInBytes = bufferFlushMaxSizeInBytes;
            return this;
        }

        public Builder setBufferFlushMaxMutations(long bufferFlushMaxMutations) {
            this.bufferFlushMaxMutations = bufferFlushMaxMutations;
            return this;
        }

        public Builder setBufferFlushIntervalMillis(long bufferFlushIntervalMillis) {
            this.bufferFlushIntervalMillis = bufferFlushIntervalMillis;
            return this;
        }

        public Builder setParallelism(Integer parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        public RedisWriteOptions build() {
            return new RedisWriteOptions(host, port, password, database, sinkTtl, connectTimeout,
                    dataType,
                    bufferFlushMaxSizeInBytes,
                    bufferFlushMaxMutations,
                    bufferFlushIntervalMillis,
                    parallelism);
        }
    }
}
