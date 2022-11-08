package org.apache.flink.streaming.connectors.redis.mapper;


public enum RedisCommand {

    SET(RedisDataType.STRING, "set"),

    GET(RedisDataType.STRING, "get"),

    HSET(RedisDataType.HASH, "hset"),

    HGET(RedisDataType.HASH, "hget"),

    DEL(RedisDataType.STRING, "del");

    private RedisDataType redisDataType;

    private String command;

    RedisCommand(RedisDataType redisDataType, String command) {
        this.redisDataType = redisDataType;
        this.command = command;
    }

    public RedisDataType getRedisDataType() {
        return redisDataType;
    }

    public String getCommand() {
        return command;
    }
}
