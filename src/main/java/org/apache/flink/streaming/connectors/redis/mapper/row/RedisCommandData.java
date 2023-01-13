package org.apache.flink.streaming.connectors.redis.mapper.row;

import com.alibaba.fastjson2.annotation.JSONType;
import org.apache.flink.streaming.connectors.redis.mapper.RedisCommand;

/**
 * @author guozixuan
 * only support string / hash
 */
public class RedisCommandData {

    private final RedisCommand redisCommand;

    private final String key;

    /**
     * set  -> String
     * hset -> Map (Set multiple hash fields)
     */
    private final Object value;

    /**
     * zset score
     */
    private final Double score;

    public RedisCommandData(RedisCommand redisCommand, String key, Object value, Double score) {
        this.redisCommand = redisCommand;
        this.key = key;
        this.value = value;
        this.score = score;
    }

    public RedisCommand getRedisCommand() {
        return redisCommand;
    }

    public String getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    public Double getScore() {
        return score;
    }

    @Override
    public String toString() {
        return "RedisCommandData{" +
                "redisCommand=" + redisCommand +
                ", key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
