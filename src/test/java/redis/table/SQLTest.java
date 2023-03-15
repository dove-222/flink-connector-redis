package redis.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisDynamicTableFactory;
import org.apache.flink.streaming.connectors.redis.mapper.RedisCommand;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.Preconditions;
import redis.TestRedisConfigBase;


public class SQLTest extends TestRedisConfigBase {

    @Test
    public void testNoPrimaryKeyInsertSQL() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        String ddl =
                "create table sink_redis(\n" +
                        "  redis_key string comment 'redisKey',\n" +
                        "  is_vip int comment '是否是会员',\n" +
                        "  total_order_amt double comment '总支付金额',\n" +
                        "  zset_score double comment '权重',\n" +
                        "  PRIMARY KEY (redis_key) NOT ENFORCED" +
                        ") with (\n" +
                        "  'connector'='redis', \n" +
                        "  'host'='127.0.0.1',\n" +
                        "  'connect.mode'='single',\n" +
                        "  'data.type'='zset',\n" +
                        "  'value.ignore-primary-key'='true', " +
                        "  'console.log.enabled'='true', " +
                        "  'sink.ttl'='100000'\n" +
                        ")";

        tEnv.executeSql(ddl);
        String sql =
                " insert into sink_redis select * from (values ('1235', 1, 188.50, 90.0))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
    }

}
