package org.apache.flink.streaming.connectors.redis.sink;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.RedisSinkFunction;
import org.apache.flink.streaming.connectors.redis.config.FlinkConfigBase;
import org.apache.flink.streaming.connectors.redis.config.RedisClusterFlinkConfig;
import org.apache.flink.streaming.connectors.redis.config.RedisSingleFlinkConfig;
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

    public RedisDynamicTableSink(
            TableSchema tableSchema,
            ReadableConfig options,
            RedisWriteOptions redisWriteOptions) {
        this.tableSchema = Preconditions.checkNotNull(tableSchema, "Physical data type must not be null.");
        this.options = options;
        this.redisWriteOptions = redisWriteOptions;
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

        String fieldTerminated = options.get(FIELD_TERMINATED);
        //根据 dataType 选择对应的 mapper
        switch (this.redisWriteOptions.getDataType().toLowerCase()) {
            case "string":
                redisMapper = new RowRedisSinkMapper(RedisCommand.SET, tableSchema, fieldTerminated);
                break;
            case "hash":
                redisMapper = new RowRedisSinkMapper(RedisCommand.HSET, tableSchema, fieldTerminated);
                break;
            default:
                throw new RuntimeException("redis-connector support string / hash only.");
        }

        return SinkFunctionProvider.of(new RedisSinkFunction<RowData>(
                redisMapper,
                redisConfig,
                tableSchema,
                3,
                redisWriteOptions.getBufferFlushMaxSizeInBytes(),
                redisWriteOptions.getBufferFlushMaxMutations(),
                redisWriteOptions.getBufferFlushIntervalMillis(),
                redisWriteOptions.getSinkTtl()),
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
