package org.apache.flink.streaming.connectors.redis.mapper.row.sink;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.converter.RedisRowConverter;
import org.apache.flink.streaming.connectors.redis.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.mapper.RedisSinkMapper;
import org.apache.flink.streaming.connectors.redis.options.RedisOptions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import java.util.HashMap;
import java.util.Map;


/** base row redis mapper implement. */
public abstract class RowRedisSinkMapper implements RedisSinkMapper<RowData> {

    private RedisCommand redisCommand;

    public RowRedisSinkMapper(RedisCommand redisCommand) {
        this.redisCommand = redisCommand;
    }

    public RowRedisSinkMapper(RedisCommand redisCommand, Map<String, String> config) {
        this.redisCommand = redisCommand;
    }

    @Override
    public String getKeyFromData(RowData rowData, LogicalType logicalType, Integer keyIndex) {
        return RedisRowConverter.rowDataToString(logicalType, rowData, keyIndex);
    }

    @Override
    public String getValueFromData(RowData rowData, LogicalType logicalType, Integer valueIndex) {
        return RedisRowConverter.rowDataToString(logicalType, rowData, valueIndex);
    }

    @Override
    public String getFieldFromData(RowData rowData, LogicalType logicalType, Integer fieldIndex) {
        return RedisRowConverter.rowDataToString(logicalType, rowData, fieldIndex);
    }

    public RedisCommand getRedisCommand() {
        return redisCommand;
    }

    @Override
    public boolean equals(Object obj) {
        RedisCommand redisCommand = ((RowRedisSinkMapper) obj).redisCommand;
        return this.redisCommand == redisCommand;
    }
}
