package org.apache.flink.streaming.connectors.redis.mapper.row.lookup;


import org.apache.flink.streaming.connectors.redis.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.mapper.RedisMapper;


/** row redis mapper. @Author: jeff.zou @Date: 2022/3/7.14:59 */
public class RowRedisMapper<OUT> implements RedisMapper<OUT> {

    RedisCommand redisCommand;

    public RowRedisMapper(RedisCommand redisCommand) {
        this.redisCommand = redisCommand;
    }

    public RedisCommand getRedisCommand() {
        return redisCommand;
    }

}
