package org.apache.flink.streaming.connectors.redis.container;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * redis single mode 执行器
 */
public class RedisContainer implements RedisCommandsContainer, Closeable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisContainer.class);

    private transient RedisClient redisClient;
    protected transient StatefulRedisConnection<String, String> connection;
    protected transient RedisAsyncCommands<String, String> asyncCommands;
    protected transient RedisCommands<String, String> syncCommands;

    private transient RedisFuture redisFuture;

    private transient final boolean isAsync;

    public RedisContainer(RedisClient redisClient, boolean isAsync) {
        this.redisClient = redisClient;
        this.isAsync = isAsync;
    }

    @Override
    public void open() throws Exception {
        connection = redisClient.connect();
        if (isAsync) {
            connection.setAutoFlushCommands(false);
            asyncCommands = connection.async();
            LOG.info("open async connection!!!!");
        } else {
            syncCommands = connection.sync();
            LOG.info("open sync connection!!!!");
        }
    }

    @Override
    public void set(String key, String value) {
        try {
            if (isAsync) {
                redisFuture = asyncCommands.set(key, value);
            } else {
                syncCommands.set(key, value);
            }
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
        try {
            if (isAsync) {
                redisFuture = asyncCommands.hset(key, map);
            } else {
                syncCommands.hset(key, map);
            }
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command HSET to key {} and hashField {} error message {}",
                        key,
                        map,
                        e.getMessage());
            }
            throw e;
        }
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
    public void expire(String key, int seconds) {
        try {
            if (isAsync) {
                redisFuture = asyncCommands.expire(key, seconds);
            } else {
                syncCommands.expire(key, seconds);
            }
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
            if (isAsync) {
                redisFuture = asyncCommands.del(key);
            } else {
                syncCommands.del(key);
            }
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
        flush();

        try {
            if (redisFuture != null) {
                redisFuture.await(2, TimeUnit.SECONDS);
            }
            connection.close();
        } catch (Exception e) {
            LOG.info("", e);
        }
        redisClient.shutdown();
    }

}
