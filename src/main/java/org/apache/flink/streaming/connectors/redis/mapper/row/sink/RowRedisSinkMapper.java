package org.apache.flink.streaming.connectors.redis.mapper.row.sink;

import com.alibaba.fastjson2.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.converter.RedisRowConverter;
import org.apache.flink.streaming.connectors.redis.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.mapper.RedisSinkMapper;
import org.apache.flink.streaming.connectors.redis.mapper.row.RedisCommandData;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.Constraint;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.util.*;


/**
 * redis mapper
 * convert data
 **/
public class RowRedisSinkMapper implements RedisSinkMapper<RowData> {

    private final RedisCommand redisCommand;

    private final int keyIndex;

    private final Map<String, DataType> tableColumns;

    private final String fieldTerminated;

    private final String nullStringLiteral;

    public RowRedisSinkMapper(RedisCommand redisCommand, TableSchema tableSchema, String fieldTerminated, String nullStringLiteral) {
        this.redisCommand = redisCommand;
        Optional<UniqueConstraint> primaryKey = tableSchema.getPrimaryKey();
        this.tableColumns = new LinkedHashMap<>();

        //init redis key index and column types
        int index = 0;
        if (primaryKey.isPresent()) {
            UniqueConstraint key = primaryKey.get();
            if (key.getType() == Constraint.ConstraintType.PRIMARY_KEY) {
                List<TableColumn> tableColumns = tableSchema.getTableColumns();
                for (int i = 0; i < tableColumns.size(); i++) {
                    TableColumn column = tableColumns.get(i);
                    this.tableColumns.put(column.getName(), column.getType());
                    if (column.getName().equals(key.getColumns().get(0))) {
                        index = i;
                    }
                }
            }
        }

        this.keyIndex = index;
        this.fieldTerminated = fieldTerminated;
        this.nullStringLiteral = nullStringLiteral;
    }

    @Override
    public List<RedisCommandData> convertToValue(RowData data) {
        RowKind rowKind = data.getRowKind();
        String redisKey = data.getString(keyIndex).toString();
        if (rowKind == RowKind.DELETE) {
            return Collections.singletonList(new RedisCommandData(RedisCommand.DEL, redisKey, "", ""));
        }

        switch (redisCommand) {
            case SET:
                return StringUtils.isNotBlank(fieldTerminated)
                        ?
                        convertToCSVSet(data, redisKey)
                        :
                        convertToJsonSet(data, redisKey);
            case HSET:
                return convertToHSet(data, redisKey);
            default:
                throw new RuntimeException("redis-connector support string / hash only.");
        }
    }

    private List<RedisCommandData> convertToJsonSet(RowData data, String redisKey) {
        JSONObject json = new JSONObject();
        int i = 0;
        for (Map.Entry<String, DataType> entry : tableColumns.entrySet()) {
            String columnName = entry.getKey();
            DataType dataType = entry.getValue();
            String value = RedisRowConverter.rowDataToString(dataType.getLogicalType(), data, i++);
            json.put(columnName, checkNullString(value));
        }
        return Collections.singletonList(new RedisCommandData(redisCommand, redisKey, json.toJSONString(), ""));
    }

    private List<RedisCommandData> convertToCSVSet(RowData data, String redisKey) {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (DataType type : tableColumns.values()) {
            String value = RedisRowConverter.rowDataToString(type.getLogicalType(), data, i++);
            sb.append(checkNullString(value));
            sb.append(fieldTerminated);
        }
        sb.delete(sb.length() - fieldTerminated.length(), sb.length());
        return Collections.singletonList(new RedisCommandData(redisCommand, redisKey, sb.toString(), ""));
    }

    private List<RedisCommandData> convertToHSet(RowData data, String redisKey) {
        List<RedisCommandData> list = new ArrayList<>();
        int i = 0;
        for (Map.Entry<String, DataType> entry : this.tableColumns.entrySet()) {
            String value =
                    RedisRowConverter.rowDataToString(entry.getValue().getLogicalType(), data, i++);
            list.add(new RedisCommandData(redisCommand, redisKey, value, entry.getKey()));
        }
        return list;
    }

    private String checkNullString(String value) {
        return value == null ? nullStringLiteral : value;
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
