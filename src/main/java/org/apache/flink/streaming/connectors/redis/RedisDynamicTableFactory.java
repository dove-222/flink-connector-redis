package org.apache.flink.streaming.connectors.redis;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.options.RedisLookupOptions;
import org.apache.flink.streaming.connectors.redis.options.RedisOptions;
import org.apache.flink.streaming.connectors.redis.options.RedisWriteOptions;
import org.apache.flink.streaming.connectors.redis.sink.RedisDynamicTableSink;
import org.apache.flink.streaming.connectors.redis.source.RedisDynamicTableSource;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.streaming.connectors.redis.options.RedisOptions.*;
import static org.apache.flink.streaming.connectors.redis.options.RedisWriteOptions.*;


/**
 * SPI机制
 * 目录：resources/META-INF.services
 * 文件名：接口全类名 org.apache.flink.table.factories.Factory
 */
public class RedisDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    /**
     * factory 唯一标识
     * sql connector 中指定
     */
    @Override
    public String factoryIdentifier() {
        return "redis";
    }

    /**
     * 必要参数
     */
    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOST);
        options.add(CONNECT_MODE);
        return options;
    }

    /**
     * 需要加上 sink.buffer 相关参数，批写入
     * @return
     */
    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FactoryUtil.FORMAT); // use pre-defined option for format

        //connect
        options.add(PORT);
        options.add(PASSWORD);
        options.add(CONNECT_TIMEOUT);
        options.add(DATABASE);
        options.add(DATA_TYPE);

        //sink
        options.add(NULL_STRING_LITERAL);
        options.add(VALUE_IGNORE_PRIMARY_KEY);
        options.add(SINK_BUFFER_FLUSH_MAX_ROWS);
        options.add(SINK_BUFFER_FLUSH_INTERVAL);
        options.add(SINK_BUFFER_FLUSH_MAX_SIZE);
        options.add(SINK_TTL);
        options.add(TTL_RANDOM_RANGE);
        options.add(FIELD_TERMINATED);
        options.add(SINK_MAX_RETRIES);
        options.add(SINK_PARALLELISM);

        //log
        options.add(KAFKA_LOG_SERVE);
        options.add(KAFKA_LOG_TOPIC);
        options.add(CONSOLE_LOG_ENABLED);

        //source
        options.add(LOOKUP_CACHE_MAX_ROWS);
        options.add(LOOKUP_CACHE_TTL);
        options.add(LOOKUP_MAX_RETRIES);
        return options;
    }

    /**
     * 创建 RedisDynamicTableSource
     * @param context
     * @return
     */
    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {

        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        //format读取
        final DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
                DeserializationFormatFactory.class,
                FactoryUtil.FORMAT);

        helper.validate();

        // get the validated options
        final ReadableConfig options = helper.getOptions();

        final RedisLookupOptions redisLookupOptions = RedisOptions.getRedisLookupOptions(options);

        TableSchema schema = context.getCatalogTable().getSchema();

        Configuration c = (Configuration) context.getConfiguration();

        //boolean isDimBatchMode = c.getBoolean("is.dim.batch.mode", false);

        return new RedisDynamicTableSource(
                schema.toPhysicalRowDataType()
                , decodingFormat
                , redisLookupOptions
                , false);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {

        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        helper.validate();

        final ReadableConfig options = helper.getOptions();

        final RedisWriteOptions redisWriteOptions = RedisOptions.getRedisWriteOptions(options);

        TableSchema schema = context.getCatalogTable().getSchema();

        String databaseName = context.getObjectIdentifier().getDatabaseName();

        String tableName = context.getObjectIdentifier().getObjectName();

        return new RedisDynamicTableSink(schema, options, redisWriteOptions, databaseName, tableName);
    }
}