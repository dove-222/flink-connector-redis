package org.apache.flink.streaming.connectors.redis.mapper.row.sink;

import com.alibaba.fastjson2.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.connectors.redis.converter.RedisRowConverter;
import org.apache.flink.streaming.connectors.redis.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.mapper.RedisSinkMapper;
import org.apache.flink.streaming.connectors.redis.mapper.row.RedisCommandData;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.Constraint;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.util.*;


/**
 * redis mapper
 * convert data
 **/
public class RowRedisSinkMapper implements RedisSinkMapper<RowData> {

    private final RedisCommand redisCommand;

    private final int primaryKeyIndex;

    private final Map<String, DataType> tableColumns;

    private final String fieldTerminated;

    private final String nullStringLiteral;

    private final Boolean ignoreKey;

    private static final String SORTED_SET_SCORE = "zset_score";

    /**
     * if a hash value is null and the nullStringLiteral has been set "false", it will skip that field.
     */
    private static final String HASH_SKIP_NULL_STRING_LITERAL = "false";

    public RowRedisSinkMapper(RedisCommand redisCommand,
                              TableSchema tableSchema,
                              String fieldTerminated,
                              String nullStringLiteral,
                              Boolean ignoreKey) {

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

        this.primaryKeyIndex = index;
        this.fieldTerminated = fieldTerminated;
        this.nullStringLiteral = nullStringLiteral;
        this.ignoreKey = ignoreKey;
    }

    @Override
    public RedisCommandData convertToValue(RowData data) {
        RowKind rowKind = data.getRowKind();
        String redisKey = data.getString(primaryKeyIndex).toString();
        if (rowKind == RowKind.DELETE) {
            return new RedisCommandData(RedisCommand.DEL, redisKey, "", null);
        }

        switch (redisCommand) {
            case SADD:
            case SET:
                return StringUtils.isNotBlank(fieldTerminated)
                        ?
                        convertToCSVSet(data, redisKey)
                        :
                        convertToJsonSet(data, redisKey);
            case HSET:
                return convertToHSet(data, redisKey);
            case ZADD:
                return convertToZAdd(data, redisKey);
            default:
                throw new RuntimeException("redis-connector only support string, hash, set or zset.");
        }
    }

    private RedisCommandData convertToCSVSet(RowData data, String redisKey) {
        return new RedisCommandData(redisCommand, redisKey, convertRowToCSVString(data), null);
    }

    private RedisCommandData convertToJsonSet(RowData data, String redisKey) {
        return new RedisCommandData(redisCommand, redisKey, convertRowToJSONString(data), null);
    }

    private RedisCommandData convertToHSet(RowData data, String redisKey) {
        Map<String, String> result = new LinkedHashMap<>();
        convertRowToMap(data).forEach((columnName, value) -> {
            if (value == null && HASH_SKIP_NULL_STRING_LITERAL.equals(nullStringLiteral)) {
                //skip this field.
            } else {
                result.put(columnName, checkNullString(value));
            }
        });
        return new RedisCommandData(redisCommand, redisKey, result, null);
    }

    private RedisCommandData convertToZAdd(RowData data, String redisKey) {
        Map<String, String> columnMap = convertRowToMap(data);
        String value;
        double score = 0.0;
        boolean scoreFlag = false;
        if (StringUtils.isNotBlank(fieldTerminated)) {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, String> entry : columnMap.entrySet()) {
                if (!SORTED_SET_SCORE.equals(entry.getKey())) {
                    sb.append(checkNullString(entry.getValue())).append(fieldTerminated);
                } else {
                    scoreFlag = true;
                    score = Double.parseDouble(entry.getValue());
                }
            }
            value = sb.delete(sb.length() - fieldTerminated.length(), sb.length()).toString();
        } else {
            JSONObject jsonObject = new JSONObject();
            for (Map.Entry<String, String> entry : columnMap.entrySet()) {
                if (!SORTED_SET_SCORE.equals(entry.getKey())) {
                    jsonObject.put(entry.getKey(), checkNullString(entry.getValue()));
                } else {
                    scoreFlag = true;
                    score = Double.parseDouble(entry.getValue());
                }
            }
            value = jsonObject.toJSONString();
        }

        if (!scoreFlag) {
            throw new RuntimeException("The zset table must have a column named 'zset_score'.");
        }
        return new RedisCommandData(redisCommand, redisKey, value, score);
    }

    private String convertRowToCSVString(RowData data) {
        StringBuilder sb = new StringBuilder();
        convertRowToMap(data).forEach(
                (columnName, value) -> sb.append(checkNullString(value)).append(fieldTerminated));
        sb.delete(sb.length() - fieldTerminated.length(), sb.length());
        return sb.toString();
    }

    private String convertRowToJSONString(RowData data) {
        JSONObject jsonObject = new JSONObject();
        convertRowToMap(data).forEach(
                (columnName, value) -> jsonObject.put(columnName, checkNullString(value)));
        return jsonObject.toJSONString();
    }

    /**
     * return Map<columnName, value>
     * value could be null
     */
    private Map<String, String> convertRowToMap(RowData data) {
        Map<String, String> map = new LinkedHashMap<>();
        int i = 0;
        for (Map.Entry<String, DataType> entry : tableColumns.entrySet()) {
            // check if a key needed to be ignored
            if (ignoreKey && i == primaryKeyIndex) {
                i++;
            } else {
                String columnName = entry.getKey();
                DataType dataType = entry.getValue();
                String value = RedisRowConverter.rowDataToString(dataType.getLogicalType(), data, i++);
                map.put(columnName, value);
            }
        }
        return map;
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
