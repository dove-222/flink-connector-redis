package org.apache.flink.streaming.connectors.redis.container;

import com.alibaba.fastjson2.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.connectors.redis.mapper.row.RedisCommandData;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * @author guozixuan
 * when execute a redis command, send an message into kafka log topic.
 */
public class KafkaLogContainer {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaLogContainer.class);

    private transient Callback callback;

    private final String logTopic;

    private final String servers;

    private transient KafkaProducer<String, String> kafkaProducer;

    public KafkaLogContainer(String servers, String logTopic) {
        this.servers = servers;
        this.logTopic = logTopic;
    }

    public void open() throws Exception {
        LOG.info("kafka log container is opening.");

        //回调
        callback = (metadata, exception) -> {
            if (exception != null) {
                LOG.error("Error while sending record to Kafka: " + exception.getMessage(), exception);
            }
        };

        if (StringUtils.isBlank(servers)) {
            LOG.error("kafka bootstrap.servers can't be blank.");
            throw new IllegalArgumentException("kafka bootstrap.servers can't be blank.");
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        props.put("retries", 3);
        kafkaProducer = new KafkaProducer<String, String>(props);
    }

    public void logToKafka(String jsonString, long timestamp) {
        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>(logTopic, null, timestamp, null, jsonString);
        kafkaProducer.send(record, callback);
    }

    public void close() {
        kafkaProducer.close();
    }
}
