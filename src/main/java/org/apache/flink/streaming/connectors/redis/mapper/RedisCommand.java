package org.apache.flink.streaming.connectors.redis.mapper;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisDataType;


public enum RedisCommand {

    GET(RedisDataType.STRING),

    HGET(RedisDataType.HASH),

    ;

    private RedisDataType redisDataType;

    RedisCommand(RedisDataType redisDataType) {
        this.redisDataType = redisDataType;
    }

    public RedisDataType getRedisDataType() {
        return redisDataType;
    }
}
