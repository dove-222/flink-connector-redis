package org.apache.flink.streaming.connectors.redis.mapper;



public class RedisCommandDescription {

    private static final long serialVersionUID = 1L;

    private RedisCommand redisCommand;

    private String additionalKey;

    public RedisCommandDescription(RedisCommand redisCommand, String additionalKey) {

        this.redisCommand = redisCommand;
        this.additionalKey = additionalKey;

        if (redisCommand.getRedisDataType() == RedisDataType.HASH) {
            if (additionalKey == null) {
                throw new IllegalArgumentException("Hash should have additional key");
            }
        }
    }

    public RedisCommandDescription(RedisCommand redisCommand) {

        this(redisCommand, null);
    }

    public RedisCommand getRedisCommand() {
        return redisCommand;
    }

    public String getAdditionalKey() {
        return additionalKey;
    }
}
