package org.apache.flink.streaming.connectors.redis.sink;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.RedisSinkFunction;
import org.apache.flink.streaming.connectors.redis.config.FlinkConfigBase;
import org.apache.flink.streaming.connectors.redis.config.RedisClusterFlinkConfig;
import org.apache.flink.streaming.connectors.redis.config.RedisSingleFlinkConfig;
import org.apache.flink.streaming.connectors.redis.container.KafkaLogContainer;
import org.apache.flink.streaming.connectors.redis.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.mapper.row.sink.RowRedisSinkMapper;
import org.apache.flink.streaming.connectors.redis.options.RedisWriteOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.streaming.connectors.redis.options.RedisOptions.*;

/**
 * https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/table/sourcessinks/
 *
 * https://www.alibabacloud.com/help/zh/faq-detail/118038.htm?spm=a2c63.q38357.a3.16.48fa711fo1gVUd
 */
public class RedisDynamicTableSink implements DynamicTableSink {

    /**
     * Data type to configure the formats.
     */
    protected final TableSchema tableSchema;

    protected final RedisWriteOptions redisWriteOptions;

    protected final ReadableConfig options;

    protected final String databaseName;

    protected final String tableName;

    public RedisDynamicTableSink(
            TableSchema tableSchema,
            ReadableConfig options,
            RedisWriteOptions redisWriteOptions,
            String databaseName,
            String tableName) {
        this.tableSchema = Preconditions.checkNotNull(tableSchema, "Physical data type must not be null.");
        this.options = options;
        this.redisWriteOptions = redisWriteOptions;
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    private FlinkConfigBase getRedisConfig() {
        String mode = options.get(CONNECT_MODE);
        switch (mode) {
            case "single":
                return new RedisSingleFlinkConfig.Builder()
                        .setHost(redisWriteOptions.getHost())
                        .setPassword(redisWriteOptions.getPassword())
                        .setPort(redisWriteOptions.getPort())
                        .setDatabase(redisWriteOptions.getDatabase())
                        .setConnectTimeout(redisWriteOptions.getConnectTimeout())
                        .setAsync(redisWriteOptions.getBufferFlushMaxMutations() > 0)
                        .build();
            case "cluster":
                return new RedisClusterFlinkConfig.Builder()
                        .setHost(redisWriteOptions.getHost())
                        .setPassword(redisWriteOptions.getPassword())
                        .setPort(redisWriteOptions.getPort())
                        .setDatabase(redisWriteOptions.getDatabase())
                        .setConnectTimeout(redisWriteOptions.getConnectTimeout())
                        .setAsync(redisWriteOptions.getBufferFlushMaxMutations() > 0)
                        .build();
            default:
                throw new IllegalArgumentException("redis connect mode only support 'single' and 'cluster'");
        }
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        FlinkConfigBase redisConfig = getRedisConfig();

        RowRedisSinkMapper redisMapper;

        //如果分隔符没有指定（null），将采用 json 格式序列化
        String fieldTerminated = options.get(FIELD_TERMINATED);

        String nullStringLiteral = options.get(NULL_STRING_LITERAL);

        Boolean ignoreKey = options.get(VALUE_IGNORE_PRIMARY_KEY);

        //根据 dataType 选择对应的 mapper
        switch (this.redisWriteOptions.getDataType().toLowerCase()) {
            case "string":
                redisMapper = new RowRedisSinkMapper(RedisCommand.SET,
                        tableSchema, fieldTerminated, nullStringLiteral, ignoreKey);
                break;
            case "hash":
                redisMapper = new RowRedisSinkMapper(RedisCommand.HSET,
                        tableSchema, fieldTerminated, nullStringLiteral, ignoreKey);
                break;
            default:
                throw new RuntimeException("redis-connector support string / hash only.");
        }

        String kafkaServers = options.get(KAFKA_LOG_SERVE);
        String logTopic = options.get(KAFKA_LOG_TOPIC);
        KafkaLogContainer logContainer = null;
        if (StringUtils.isNotBlank(kafkaServers) && StringUtils.isNotBlank(logTopic)) {
            logContainer = new KafkaLogContainer(kafkaServers, logTopic);
        }

        return SinkFunctionProvider.of(new RedisSinkFunction<RowData>(
                redisMapper,
                redisConfig,
                logContainer,
                options.get(SINK_MAX_RETRIES),
                redisWriteOptions.getBufferFlushMaxSizeInBytes(),
                redisWriteOptions.getBufferFlushMaxMutations(),
                redisWriteOptions.getBufferFlushIntervalMillis(),
                redisWriteOptions.getSinkTtl(),
                redisWriteOptions.getHost(),
                databaseName,
                tableName,
                options.get(CONSOLE_LOG_ENABLED)),
                redisWriteOptions.getParallelism());
    }

    @Override
    public DynamicTableSink copy() {
        return null;
    }

    @Override
    public String asSummaryString() {
        return "Redis";
    }
}
