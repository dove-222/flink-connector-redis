package org.apache.flink.streaming.connectors.redis.mapper;


import org.apache.flink.streaming.connectors.redis.mapper.row.RedisCommandData;

import java.util.List;

/**
 * @param <T>
 */
public interface RedisSinkMapper<T> extends RedisMapper<T> {

    /**
     * Converts the input record into Redis value.
     *
     * @param data
     * @return key
     */
    RedisCommandData convertToValue(T data);
}
