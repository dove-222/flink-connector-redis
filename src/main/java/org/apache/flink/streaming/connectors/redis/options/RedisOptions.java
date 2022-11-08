package org.apache.flink.streaming.connectors.redis.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.Preconditions;

import java.time.Duration;
import java.util.stream.IntStream;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;

/**
 * @author guozixuan
 * redis options item
 */
public class RedisOptions {

    public static final ConfigOption<String> HOST = ConfigOptions
            .key("host")
            .stringType()
            .noDefaultValue()
            .withDescription("Optional host for connect to redis");

    public static final ConfigOption<String> PASSWORD = ConfigOptions
            .key("password")
            .stringType()
            .noDefaultValue()
            .withDescription("Optional password for connect to redis");

    public static final ConfigOption<Integer> PORT = ConfigOptions
            .key("port")
            .intType()
            .defaultValue(6379)
            .withDescription("Optional port for connect to redis");

    public static final ConfigOption<Integer> DATABASE = ConfigOptions
            .key("database")
            .intType()
            .defaultValue(0)
            .withDescription("Optional database for connect to redis");

    public static final ConfigOption<String> DATA_TYPE = ConfigOptions
            .key("data.type")
            .stringType()
            .defaultValue("string")
            .withDescription("data mode for insert to redis. choose string or hash.");

    public static final ConfigOption<String> FIELD_TERMINATED = ConfigOptions
            .key("field.terminated")
            .stringType()
            .defaultValue(",")
            .withDescription("concat all data fields with this character.");

    public static final ConfigOption<String> CONNECT_MODE = ConfigOptions
            .key("connect.mode")
            .stringType()
            .noDefaultValue()
            .withDescription("Optional redis-mode for connect to redis");

    public static final ConfigOption<Integer> CONNECT_TIMEOUT = ConfigOptions
            .key("connect.timeout")
            .intType()
            .defaultValue(3000)
            .withDescription("Optional timeout for connect to redis");

    public static final ConfigOption<Integer> SINK_TTL =
            ConfigOptions.key("sink.ttl")
                    .intType()
                    .defaultValue(24 * 60 * 60)
                    .withDescription("sink the cache time to live.");

    public static final ConfigOption<String> KAFKA_LOG_SERVE = ConfigOptions
            .key("kafka.log.servers")
            .stringType()
            .noDefaultValue()
            .withDescription("each redis command would product a log to kafka topic.");

    public static final ConfigOption<String> KAFKA_LOG_TOPIC = ConfigOptions
            .key("kafka.log.topic")
            .stringType()
            .defaultValue("flink_redis_log")
            .withDescription("each redis command would product a log to kafka topic.");

    public static final ConfigOption<MemorySize> SINK_BUFFER_FLUSH_MAX_SIZE =
            ConfigOptions.key("sink.buffer-flush.max-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("2mb"))
                    .withDescription(
                            "Writing option, maximum size in memory of buffered rows for each "
                                    + "writing request. This can improve performance for writing data to Redis database, "
                                    + "but may increase the latency. Can be set to '0' to disable it. ");

    public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS =
            ConfigOptions.key("sink.buffer-flush.max-rows")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Writing option, maximum number of rows to buffer for each writing request. "
                                    + "This can improve performance for writing data to Redis database, but may increase the latency. "
                                    + "Can be set to '0' to disable it.");

    public static final ConfigOption<Duration> SINK_BUFFER_FLUSH_INTERVAL =
            ConfigOptions.key("sink.buffer-flush.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription(
                            "Writing option, the interval to flush any buffered rows. "
                                    + "This can improve performance for writing data to Redis database, but may increase the latency. "
                                    + "Can be set to '0' to disable it. Note, both 'sink.buffer-flush.max-size' and 'sink.buffer-flush.max-rows' "
                                    + "can be set to '0' with the flush interval set allowing for complete async processing of buffered actions.");

    public static final ConfigOption<Integer> SINK_MAX_RETRIES =
            ConfigOptions.key("sink.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("when sink failed, retry.");

    public static final ConfigOption<Integer> SINK_PARALLELISM = FactoryUtil.SINK_PARALLELISM;

    public static final ConfigOption<Boolean> LOOKUP_ASYNC =
            ConfigOptions.key("lookup.async")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("whether to set async lookup.");

    public static final ConfigOption<Long> LOOKUP_CACHE_MAX_ROWS =
            ConfigOptions.key("lookup.cache.max-rows")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription(
                            "the max number of rows of lookup cache, over this value, the oldest rows will "
                                    + "be eliminated. \"cache.max-rows\" and \"cache.ttl\" options must all be "
                                    + "specified if any of them is "
                                    + "specified. Cache is not enabled as default.");

    public static final ConfigOption<Duration> LOOKUP_CACHE_TTL =
            ConfigOptions.key("lookup.cache.ttl")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(0))
                    .withDescription("the cache time to live.");

    public static final ConfigOption<Integer> LOOKUP_MAX_RETRIES =
            ConfigOptions.key("lookup.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("the max retry times if lookup database failed.");

    public static RedisLookupOptions getRedisLookupOptions(ReadableConfig tableOptions) {
        return RedisLookupOptions
                .builder()
                .setLookupAsync(tableOptions.get(LOOKUP_ASYNC))
                .setMaxRetryTimes(tableOptions.get(LOOKUP_MAX_RETRIES))
                .setCacheExpireMs(tableOptions.get(LOOKUP_CACHE_TTL).toMillis())
                .setCacheMaxSize(tableOptions.get(LOOKUP_CACHE_MAX_ROWS))
                .setHostname(tableOptions.get(HOST))
                .setPort(tableOptions.get(PORT))
                .build();
    }

    public static RedisWriteOptions getRedisWriteOptions(ReadableConfig tableOptions) {
        return RedisWriteOptions
                .builder()
                .setHost(tableOptions.get(HOST))
                .setPort(tableOptions.get(PORT))
                .setPassword(tableOptions.get(PASSWORD))
                .setSinkTtl(tableOptions.get(SINK_TTL))
                .setDataType(tableOptions.get(DATA_TYPE))
                .setConnectTimeout(tableOptions.get(CONNECT_TIMEOUT))
                .setBufferFlushMaxSizeInBytes(tableOptions.get(SINK_BUFFER_FLUSH_MAX_SIZE).getBytes())
                .setBufferFlushMaxMutations(tableOptions.get(SINK_BUFFER_FLUSH_MAX_ROWS))
                .setBufferFlushIntervalMillis(tableOptions.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis())
                .setParallelism(tableOptions.getOptional(SINK_PARALLELISM).orElse(null))
                .build();
    }

    /**
     * Creates an array of indices that determine which physical fields of the table schema to
     * include in the value format.
     *
     * for more information.
     */
    public static int[] createValueFormatProjection(
            DataType physicalDataType) {
        final LogicalType physicalType = physicalDataType.getLogicalType();
        Preconditions.checkArgument(
                hasRoot(physicalType, LogicalTypeRoot.ROW), "Row data type expected.");
        final int physicalFieldCount = LogicalTypeChecks.getFieldCount(physicalType);
        final IntStream physicalFields = IntStream.range(0, physicalFieldCount);

        return physicalFields.toArray();
    }

}
