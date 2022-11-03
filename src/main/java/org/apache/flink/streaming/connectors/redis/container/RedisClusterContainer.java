package org.apache.flink.streaming.connectors.redis.container;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
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
    protected transient RedisAdvancedClusterAsyncCommands clusterAsyncCommands;
    protected transient RedisFuture redisFuture;

    /**
     * 初始化 redis 命令执行器
     * @param redisClusterClient
     */
    public RedisClusterContainer(RedisClusterClient redisClusterClient) {
        Objects.requireNonNull(redisClusterClient, "redisClusterClient can not be null");
        this.redisClusterClient = redisClusterClient;
    }

    @Override
    public void open() throws Exception {
        connection = redisClusterClient.connect();
        clusterAsyncCommands = connection.async();
        LOG.info("open async connection!!!!");
    }

    @Override
    public void set(String key, String value) {
        try {
            redisFuture = clusterAsyncCommands.set(key, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command SET to key {} error message {}",
                        key,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public RedisFuture<String> get(String key) {
        RedisFuture<String> result;
        try {
            redisFuture = result = clusterAsyncCommands.get(key);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command hget to key {} error message {}",
                        key,
                        e.getMessage());
            }
            throw e;
        }
        return result;
    }

    @Override
    public void hset(String key, String field, String value) {
        try {
            redisFuture = clusterAsyncCommands.hset(key, field, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command HSET to hash {} of key {} error message {}",
                        field,
                        key,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public RedisFuture<String> hget(String key, String field) {
        RedisFuture<String> result;
        try {
            redisFuture = result = clusterAsyncCommands.hget(key, field);
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
    public void expire(String key, int seconds) {
        try {
            redisFuture = clusterAsyncCommands.expire(key, Duration.ofSeconds(seconds));
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command exists to key {}  seconds {} error message {}",
                        key,
                        seconds,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void del(String key) {
        try {
            redisFuture = clusterAsyncCommands.del(key);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command del to key {} error message {}",
                        key,
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
