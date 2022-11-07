package org.apache.flink.streaming.connectors.redis;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.redis.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.container.RedisCommandsContainerBuilder;
import org.apache.flink.streaming.connectors.redis.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.config.FlinkConfigBase;
import org.apache.flink.streaming.connectors.redis.mapper.RedisSinkMapper;
import org.apache.flink.streaming.connectors.redis.mapper.row.RedisCommandData;
import org.apache.flink.streaming.connectors.redis.mapper.row.sink.RowRedisSinkMapper;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author guozixuan
 */
@Internal
public class RedisSinkFunction<IN> extends RichSinkFunction<IN> implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSinkFunction.class);

    private RedisSinkMapper<IN> redisSinkMapper;

    private FlinkConfigBase flinkConfigBase;
    private RedisCommandsContainer redisCommandsContainer;

    private final int maxRetryTimes;
    private List<DataType> columnDataTypes;

    private final long bufferFlushMaxSizeInBytes;
    private final long bufferFlushMaxMutations;
    private final long bufferFlushIntervalMillis;
    private final Integer ttl;

    private transient ScheduledExecutorService executor;
    private transient ScheduledFuture scheduledFuture;
    private transient AtomicLong numPendingRequests;

    private transient volatile boolean closed = false;

    public RedisSinkFunction(RedisSinkMapper<IN> redisSinkMapper,
                             FlinkConfigBase flinkConfigBase,
                             TableSchema tableSchema,
                             int maxRetryTimes,
                             long bufferFlushMaxSizeInBytes,
                             long bufferFlushMaxMutations,
                             long bufferFlushIntervalMillis,
                             Integer ttl) {
        this.redisSinkMapper = redisSinkMapper;
        this.flinkConfigBase = flinkConfigBase;
        this.columnDataTypes = Arrays.asList(tableSchema.getFieldDataTypes());
        this.maxRetryTimes = maxRetryTimes;
        this.bufferFlushMaxSizeInBytes = bufferFlushMaxSizeInBytes;
        this.bufferFlushMaxMutations = bufferFlushMaxMutations;
        this.bufferFlushIntervalMillis = bufferFlushIntervalMillis;
        this.ttl = ttl;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        try {
            this.redisCommandsContainer = RedisCommandsContainerBuilder.build(this.flinkConfigBase);
            this.redisCommandsContainer.open();
            this.numPendingRequests = new AtomicLong(0);

            if (bufferFlushIntervalMillis > 0 && bufferFlushIntervalMillis != 1) {
                this.executor =
                        Executors.newScheduledThreadPool(
                                1, new ExecutorThreadFactory("redis-upsert-sink-flusher"));
                this.scheduledFuture =
                        this.executor.scheduleWithFixedDelay(
                                () -> {
                                    if (closed) {
                                        return;
                                    }
                                    try {
                                        flush();
                                    } catch (Exception e) {
                                        LOG.error("redis flush failed!", e);
                                        throw e;
                                    }
                                },
                                bufferFlushIntervalMillis,
                                bufferFlushIntervalMillis,
                                TimeUnit.MILLISECONDS);
            }

            LOG.info(
                    "{} success to create redis container:{}",
                    Thread.currentThread().getId(),
                    this.flinkConfigBase.toString());
        } catch (Exception e) {
            LOG.error("Redis has not been properly initialized: ", e);
            throw e;
        }
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {

        List<RedisCommandData> commands = redisSinkMapper.convertToValue(value);

        for (int i = 0; i <= maxRetryTimes; i++) {
            try {
                execute(commands);
                break;
            } catch (UnsupportedOperationException e) {
                throw e;
            } catch (Exception e1) {
                LOG.error("sink redis error, retry times:{}", i, e1);
                if (i >= this.maxRetryTimes) {
                    throw new RuntimeException("sink redis error ", e1);
                }
                Thread.sleep(500L * i);
            }
        }

        if (bufferFlushMaxMutations > 0 && numPendingRequests.incrementAndGet() >= bufferFlushMaxMutations) {
            flush();
        }
    }

    /**
     * 由于 lettuce 是线程安全的，所有实例共用一个连接，没有业务隔离性
     * 要隔离需要实现 @BatchExecutor
     * flink sql 使用比较单一，因此直接 connection.flushCommands
     * flush 操作会
     */
    private void flush() {
        redisCommandsContainer.flush();
        numPendingRequests.set(0);
    }

    private void execute(List<RedisCommandData> data) {
        for (RedisCommandData item : data) {
            RedisCommand redisCommand = item.getRedisCommand();
            switch (redisCommand) {
                case SET:
                    this.redisCommandsContainer.set(item.getKey(), item.getValue());
                    break;
                case HSET:
                    this.redisCommandsContainer.hset(item.getKey(), item.getField(), item.getValue());
                    break;
                case DEL:
                    this.redisCommandsContainer.del(item.getKey());
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Cannot process such data type: " + redisCommand);
            }

            if (ttl != 0) {
                this.redisCommandsContainer.expire(item.getKey(), ttl);
            }
        }
    }

    /**
     * when a checkpoint triggered, flush
     * @param functionSnapshotContext
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        while (numPendingRequests.get() != 0) {
            flush();
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        // nothing to do.
    }

    @Override
    public void close() throws IOException {
        closed = true;

        flush();

        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
            if (executor != null) {
                executor.shutdownNow();
            }
        }

        if (redisCommandsContainer != null) {
            try {
                redisCommandsContainer.close();
            } catch (IOException e) {
                LOG.warn("Exception occurs while closing redis Connection.", e);
            }
            this.redisCommandsContainer = null;
        }
    }

}
