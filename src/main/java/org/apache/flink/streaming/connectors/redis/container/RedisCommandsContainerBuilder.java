package org.apache.flink.streaming.connectors.redis.container;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.connectors.redis.config.FlinkConfigBase;
import org.apache.flink.streaming.connectors.redis.config.RedisClusterFlinkConfig;
import org.apache.flink.streaming.connectors.redis.config.RedisSingleFlinkConfig;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * 通过配置构建 redis 执行器
 */
public class RedisCommandsContainerBuilder {

    public static RedisCommandsContainer build(FlinkConfigBase config) {
        if (config instanceof RedisSingleFlinkConfig) {
            return RedisCommandsContainerBuilder.build((RedisSingleFlinkConfig) config);
        } else if (config instanceof RedisClusterFlinkConfig) {
            return RedisCommandsContainerBuilder.build((RedisClusterFlinkConfig) config);
        } else {
            throw new IllegalArgumentException("redis configuration not found");
        }
    }

    public static RedisCommandsContainer build(RedisSingleFlinkConfig config) {
        Objects.requireNonNull(config, "Redis single config should not be Null");

        RedisURI.Builder builder =
                RedisURI.builder()
                        .withHost(config.getHost())
                        .withPort(config.getPort())
                        .withDatabase(config.getDatabase())
                        .withTimeout(Duration.ofMillis(config.getConnectTimeout()));
        if (StringUtils.isNotBlank(config.getPassword())) {
            builder.withPassword(config.getPassword().toCharArray());
        }

        return new RedisContainer(RedisClient.create(builder.build()));
    }

    public static RedisCommandsContainer build(RedisClusterFlinkConfig config) {
        Objects.requireNonNull(config, "Redis pool config should not be Null");

        String[] nodesInfo = config.getNodesInfo().split(",");

        List<RedisURI> redisURIS =
                Arrays.stream(nodesInfo)
                        .map(node -> {
                            String[] info = node.split(":");
                            RedisURI.Builder builder =
                                    RedisURI.builder()
                                            .withHost(info[0])
                                            .withPort(Integer.parseInt(info[1]))
                                            .withTimeout(Duration.ofMillis(config.getConnectTimeout()));
                            if (StringUtils.isNotBlank(config.getPassword())) {
                                builder.withPassword(config.getPassword().toCharArray());
                            }
                            return builder.build();
                        })
                        .collect(Collectors.toList());

        RedisClusterClient clusterClient = RedisClusterClient.create(redisURIS);

        //redis cluster topology refresh
        ClusterTopologyRefreshOptions topologyRefreshOptions =
                ClusterTopologyRefreshOptions.builder()
                        .enableAdaptiveRefreshTrigger(
                                ClusterTopologyRefreshOptions.RefreshTrigger.MOVED_REDIRECT,
                                ClusterTopologyRefreshOptions.RefreshTrigger.PERSISTENT_RECONNECTS)
                        .adaptiveRefreshTriggersTimeout(Duration.ofSeconds(10L))
                        .build();

        clusterClient.setOptions(
                ClusterClientOptions.builder()
                        .topologyRefreshOptions(topologyRefreshOptions)
                        .build());

        return new RedisClusterContainer(clusterClient);
    }

}
