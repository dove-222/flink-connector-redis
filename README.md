
### 项目介绍

Redis 连接器支持读取和写入 Redis 单机或集群。本文档介绍如何使用 Redis 连接器基于 Redis 进行 SQL 查询。
Redis 连接器在 upsert 模式下运行，并且使用 DDL 中定义的主键作为 key 与 redis 集群交换更新操作消息。如果没有声明主键，Redis 连接器默认取第一个字段作为主键。

### 项目简介

  本项目基于开源项目 https://github.com/jeff-zou/flink-connector-redis 开发，相对调整的内容有：

```
1. 通过参数控制 Lettuce flush 时机
2. 支持随机 ttl 
3. 增加Kafka Log，每条操作都有日志可查
4. 增加buffer-flush.max-rows、buffer-flush.interval 参数
```

   
### 如何部署
   需要在 flink/lib 目录下添加以下jar包
```
   lettuce-core-6.2.1.RELEASE.jar
   reactor-core-3.4.23.jar
   flink-connector-redis-1.1.0-1.12.jar
```
   
### 如何使用 Redis 表
   所有的 Redis 表必须定义主键。只能声明一个字段为主键
```
   create table sink_redis(
   username VARCHAR,
   password VARCHAR,
   topic VARCHAR,
   RIMARY KEY (username) NOT ENFORCED  -- redis key 字段
   ) with (
   'connector'='redis',
   'host'='localhost',
   'password'='***',
   'connect.mode'='single',
   'data.type'='string',
   'sink.ttl'='3600'
   )

insert into sink_redis
select * from (values ('zixuan.guo','IFHnjsdafu','sink_topic'))

-- 1 当 data.type = string 时

-- 1.1 不指定 field.terminated 的情况下，默认使用 json 格式
set 'zixuan.guo' "{"username":"zixuan.guo","password":"IFHnjsdafu","topic":"sink_topic"}"

-- 1.2 如果指定了 field.terminated，将使用 csv 格式
set 'zixuan.guo' "zixuan.guo,IFHnjsdafu,sink_topic"

-- 1.3 'zset'表中必须包含字段 'zset_score' double comment '排序权重'


-- 2 当 data.type = hash 时

-- 2.1 不支持field.terminated，所有字段都转换成 string 写入

-- 2.2 每个字段都会作为 redis 的 field，使用 hset 命令写入
    hset 'zixuan.guo'  username  'zixuan.guo'   password  'IFHnjsdafu'  topic   'sink_topic'

-- 2.3 在某种情况下，如果希望 hash 格式对 null 值不做任何处理（默认会写入"null"字符串），请配置null-string-literal = false！


-- 3 开启 redis sink log 后，日志格式如下
{
"command":"set",
"key":"zixuan.guo",
"value":"IFHnjsdafu,sink_topic",
"field":"",
"ts":1667904836499,
"database":"default_database",
"table":"sink_redis",
"host":"127.0.0.1"
}
```

### 连接器参数

| 参数                  | 是否必选    | 默认参数    | 数据类型   |描述                                                                                            |
| --------------------- | ------ | ------- |----------------------|----------------------------------------------------------------------------|
|connector                    |  必选   |  (none)        |    String     |    指定使用的连接器, 填 "redis"   |
|host                         |  必选   |  (none)        |    String     |    连接的 Redis 地址   |
|connect.mode                 |  必选   |  (none)        |    String     |    Redis 连接模式。支持的值有："single" 单机模式，"cluster" 集群模式   |
|port                         |  可选   |  6379          |    String     |    Redis 的端口。   |
|password                     |  可选   |  (none)        |    String     |    Redis 连接密码   |
|data.type                    |  可选   |  string        |    String     |    Redis 数据类型，支持的值有："string"，对应set，get方法；"hash"，对应hset，hget方法；"set/zset"   |
|field.terminated             |  可选   |  (none)        |    String     |    此参数决定了sink时使用json或csv。若不指定，value值将使用json序列化器；若指定，则使用指定的字符作为分隔符，生成csv格式写入，详情请见上方例子。此参数在data.type='string'时生效，将在下一个版本弃用  |
|value.ignore-primary-key     |  可选   |  false         |    Boolean    |    写入redis的值是否需要忽略主键。如果设置为'true'，value将不包含主键值  |
|null-string-literal          |  可选   |   null         |    String     |    当字符串值为 null 时的存储形式，默认存成 "null" 字符串。如果 data.type=hash 并且此参数设置为"false"，连接器将会对 value 为 null 的 field 直接跳过，不做任何处理  |
|database                     |  可选   |   0            |    Integer    |   Redis 选择数据库  |
|connect.timeout              |  可选   |   3000         |    Integer    |   连接 Redis 的超时时间  |
|console.log.enabled          |  可选   |   false        |    Boolean    |   开启控制台日志，仅供调试使用   |
|kafka.log.servers            |  可选   |  (none)        |    String     |   设置此参数可开启 redis sink log。每条 redis 命令日志都将发送至此 kafka 集群中。默认不开启  |
|kafka.log.topic              |  可选   |  flink_redis_log |    String   |   redis sink log 的 topic，此参数与 kafka.log.servers 搭配使用  |
|sink.buffer-flush.max-rows   |  可选   |  1000          |    Integer    |   写入的参数选项。 每次写入请求缓存的最大行数。它能提升写入 Redis 数据库的性能，但是也可能增加延迟。设置为 "0" 关闭此选项，并采用同步方式写入  |
|sink.buffer-flush.interval   |  可选   |   1s           |    Duration   |   写入的参数选项。刷写缓存行的间隔。它能提升写入 Redis 数据库的性能，但是也可能增加延迟。设置为 "0" 关闭此选项  |
|sink.ttl                     |  可选   |  86400         |    Integer    |   Redis 写入 key 的 ttl（单位：秒），如果设置为0，则不设置过期时间  |
|ttl.random.range             |  可选   |  0（关闭）       |    String     |   设置一个正整数的随机数范围，如1-100，在写入每条数据时，都会在此范围内生成一个随机数，如50，则最终ttl=sink.ttl+50（单位：秒）。在sink.ttl != 0时有效，设置为 "0" 关闭此选项  |
|sink.max-retries             |  可选   |  3             |    Integer    |   Redis 写入数据的重试次数  |
|sink.parallelism             |  可选   |  (none)        |    Integer    |    为 Redis sink operator 定义并行度。默认情况下，并行度由框架决定，和链在一起的上游 operator 一样   |


### 数据类型转换

| flink type   | redis row converter                                          |
| ------------ | ------------------------------------------------------------ |
| CHAR         | String                                                       |
| VARCHAR      | String                                                       |
| String       | String                                                       |
| BOOLEAN      | String String.valueOf(boolean val) <br/>boolean Boolean.valueOf(String str) |
| BINARY       | String Base64.getEncoder().encodeToString  <br/>byte[]   Base64.getDecoder().decode(String str)             |
| VARBINARY    | String Base64.getEncoder().encodeToString  <br/>byte[]   Base64.getDecoder().decode(String str)                 |
| DECIMAL      | String  BigDecimal.toString <br/>DecimalData DecimalData.fromBigDecimal(new BigDecimal(String str),int precision, int scale)                               |
| TINYINT      | String String.valueOf(byte val)  <br/>byte Byte.valueOf(String str)                         |
| SMALLINT     | String String.valueOf(short val) <br/>short Short.valueOf(String str)                          |
| INTEGER      | String String.valueOf(int val)  <br/>int Integer.valueOf(String str)                          |
| DATE         | String the day from epoch as int <br/>date show as 2022-01-01                             |
| TIME         | String the millisecond from 0'clock as int <br/>time show as 04:04:01.023                         |
| BIGINT       | String String.valueOf(long val) <br/>long Long.valueOf(String str)                            |
| FLOAT        | String String.valueOf(float val) <br/>float Float.valueOf(String str)                            |
| DOUBLE       | String String.valueOf(double val) <br/>double Double.valueOf(String str)                           |
| TIMESTAMP | String the millisecond from epoch as long <br/>timestamp TimeStampData.fromEpochMillis(Long.valueOf(String str))                    |

