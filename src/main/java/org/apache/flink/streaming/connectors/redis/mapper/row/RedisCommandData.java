package org.apache.flink.streaming.connectors.redis.mapper.row;

import org.apache.flink.streaming.connectors.redis.mapper.RedisCommand;

/**
 * @author guozixuan
 * only support string / hash
 */
public class RedisCommandData {

    private final RedisCommand redisCommand;

    private final String key;

    private final String value;

    private final String field;

    public RedisCommandData(RedisCommand redisCommand, String key, String value, String field) {
        this.redisCommand = redisCommand;
        this.key = key;
        this.value = value;
        this.field = field;
    }

    public RedisCommand getRedisCommand() {
        return redisCommand;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public String getField() {
        return field;
    }

    @Override
    public String toString() {
        return "RedisCommandData{" +
                "redisCommand=" + redisCommand +
                ", key='" + key + '\'' +
                ", value='" + value + '\'' +
                ", field='" + field + '\'' +
                '}';
    }
}
