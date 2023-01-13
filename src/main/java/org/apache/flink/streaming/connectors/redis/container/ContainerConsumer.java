package org.apache.flink.streaming.connectors.redis.container;

@FunctionalInterface
public interface ContainerConsumer<K, V, S> {

    void execute(K k, V v, S s);
}
