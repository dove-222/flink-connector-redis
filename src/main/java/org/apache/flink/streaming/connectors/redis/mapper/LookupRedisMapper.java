package org.apache.flink.streaming.connectors.redis.mapper;


import com.google.common.base.Joiner;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;

import java.io.IOException;


public class LookupRedisMapper extends AbstractDeserializationSchema<RowData> implements SerializationSchema<Object[]> {


    private DeserializationSchema<RowData> valueDeserializationSchema;

    public LookupRedisMapper(DeserializationSchema<RowData> valueDeserializationSchema) {

        this.valueDeserializationSchema = valueDeserializationSchema;

    }

    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.GET);
    }

    @Override
    public RowData deserialize(byte[] message) {
        try {
            return this.valueDeserializationSchema.deserialize(message);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] serialize(Object[] element) {
        return Joiner.on(":").join(element).getBytes();
    }
}
