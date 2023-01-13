package org.apache.flink.streaming.connectors.redis.container;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * @author guozixuan
 * redis cluster mode 执行器
 */
public class RedisClusterContainer implements RedisCommandsContainer, Closeable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisClusterContainer.class);

    protected transient RedisClusterClient redisClusterClient;

    protected transient StatefulRedisClusterConnection<String, String> connection;
    protected transient RedisAdvancedClusterAsyncCommands<String, String> asyncCommands;
    protected transient RedisAdvancedClusterCommands<String, String> syncCommands;
    protected transient RedisFuture redisFuture;

    private transient final boolean isAsync;

    /**
     * 初始化 redis 命令执行器
     * @param redisClusterClient
     */
    public RedisClusterContainer(RedisClusterClient redisClusterClient, boolean isAsync) {
        Objects.requireNonNull(redisClusterClient, "redisClusterClient can not be null");
        this.redisClusterClient = redisClusterClient;
        this.isAsync = isAsync;
    }

    @Override
    public void open() throws Exception {
        connection = redisClusterClient.connect();
        if (isAsync) {
            connection.setAutoFlushCommands(false);
            asyncCommands = connection.async();
            LOG.info("open async cluster connection!!");
        } else {
            syncCommands = connection.sync();
            LOG.info("open sync cluster connection!!");
        }
    }

    @Override
    public void set(String key, String value) {
        this.execute(key, value, null, (k, v, s) -> {
            if (isAsync) {
                redisFuture = asyncCommands.set(key, value);
            } else {
                syncCommands.set(key, value);
            }
        });
    }

    @Override
    public RedisFuture<String> get(String key) {
        RedisFuture<String> result;
        try {
            redisFuture = result = asyncCommands.get(key);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command get to key {} error message {}",
                        key,
                        e.getMessage());
            }
            throw e;
        }
        return result;
    }

    @Override
    public void hset(String key, Map<String, String> map) {
        this.execute(key, map, null, (k, v, s) -> {
            if (isAsync) {
                redisFuture = asyncCommands.hset(key, map);
            } else {
                syncCommands.hset(key, map);
            }
        });
    }

    @Override
    public RedisFuture<String> hget(String key, String field) {
        RedisFuture<String> result;
        try {
            redisFuture = result = asyncCommands.hget(key, field);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command hget to key {} with field {} error message {}",
                        key,
                        field,
                        e.getMessage());
            }
            throw e;
        }
        return result;
    }

    @Override
    public void sadd(String key, String value) {
        this.execute(key, value, null, (k, v, s) -> {
            if (isAsync) {
                redisFuture = asyncCommands.sadd(key, value);
            } else {
                syncCommands.sadd(key, value);
            }
        });
    }

    @Override
    public void zadd(String key, double score, String value) {
        this.execute(key, value, score, (k, v, s) -> {
            if (isAsync) {
                redisFuture = asyncCommands.zadd(key, score, value);
            } else {
                syncCommands.zadd(key, score, value);
            }
        });
    }

    @Override
    public void expire(String key, int seconds) {
        this.execute(key, seconds, null, (k, v, s) -> {
            if (isAsync) {
                redisFuture = asyncCommands.expire(key, Duration.ofSeconds(seconds));
            } else {
                syncCommands.expire(key, Duration.ofSeconds(seconds));
            }
        });
    }

    @Override
    public void del(String key) {
        this.execute(key, null, null, (k, v, s) -> {
            if (isAsync) {
                redisFuture = asyncCommands.del(key);
            } else {
                syncCommands.del(key);
            }
        });
    }

    @Override
    public void execute(String key, Object value, Double score, ContainerConsumer<String, Object, Double> action) {
        try {
            action.execute(key, value, score);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message for key {}, value {}, error message {}",
                        key,
                        value,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void flush() {
        connection.flushCommands();
    }

    @Override
    public void close() throws IOException {
        flush();

        try {
            if (redisFuture != null) {
                redisFuture.await(2, TimeUnit.SECONDS);
            }
            this.connection.close();
        } catch (Exception e) {
            LOG.error("", e);
        }
        this.redisClusterClient.shutdown();
    }

}
