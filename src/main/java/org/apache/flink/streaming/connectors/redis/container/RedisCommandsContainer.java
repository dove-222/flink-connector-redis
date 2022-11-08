/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.redis.container;

import io.lettuce.core.RedisFuture;

import java.io.Closeable;
import java.io.Serializable;
import java.util.List;

/**
 * The container for all available Redis commands.
 */
public interface RedisCommandsContainer extends Closeable, Serializable {

    void open() throws Exception;

    void set(String key, String value);

    RedisFuture<String> get(String key);

    void hset(String key, String field, String value);

    RedisFuture<String> hget(String key, String field);

    void expire(String key, int seconds);

    void del(String key);

    /**
     * 由于 lettuce 是线程安全的，所有实例共用一个连接，因此 connection.flushCommands 是全局配置
     * 如果要实现业务隔离，需要使用 @BatchExecutor
     */
    void flush();
}
