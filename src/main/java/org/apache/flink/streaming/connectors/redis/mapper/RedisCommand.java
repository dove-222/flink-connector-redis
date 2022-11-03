package org.apache.flink.streaming.connectors.redis.mapper;



public enum RedisCommand {

    SET(RedisDataType.STRING),

    GET(RedisDataType.STRING),

    HSET(RedisDataType.HASH),

    HGET(RedisDataType.HASH),

    DEL(RedisDataType.STRING);

    private RedisDataType redisDataType;

    RedisCommand(RedisDataType redisDataType) {
        this.redisDataType = redisDataType;
    }

    public RedisDataType getRedisDataType() {
        return redisDataType;
    }
}
