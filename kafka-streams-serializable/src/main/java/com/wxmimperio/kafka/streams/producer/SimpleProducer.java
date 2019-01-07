package com.wxmimperio.kafka.streams.producer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wxmimperio.kafka.streams.commons.SchemaUtils;
import org.apache.avro.Schema;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class SimpleProducer {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleProducer.class);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final static String BOOTSTRAP_SERVERS = "192.168.1.110:9092";
    private final static String ACKS = "1";
    private final static String topic = "stream-test1";

    private static final ThreadLocal<SimpleDateFormat> descFormat = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
    };

    private Properties props() {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.ACKS_CONFIG, ACKS);
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return props;
    }

    private Producer<String, String> getProducer() {
        return new KafkaProducer<>(props());
    }

    private void process(Producer producer, String key, byte[] value) {
        final ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, value);
        Future<RecordMetadata> future = producer.send(record);
        try {
            LOG.info("Topic = {}, Offset = {}, Partition = {}", future.get().toString(), future.get().offset(), future.get().partition());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private void start() {
        long index = 0L;
        try (Producer producer = getProducer()) {
            Schema schema = SchemaUtils.getSchema("E:\\coding\\github\\kafka-streams-best-practice\\kafka-streams-serializable\\src\\main\\resources\\test.avro");
            while (!closed.get()) {

                JSONObject message = new JSONObject();
                message.put("name", UUID.randomUUID().toString());

                // schema
                process(producer, Long.toString(System.currentTimeMillis()), SchemaUtils.avroSerializedValue(schema,message));

                //process(producer, Long.toString(System.currentTimeMillis()), message.toString().getBytes());
                Thread.sleep(5000);
                System.out.println(message);

              /*  if (index > 10) {
                    closed.set(true);
                }*/
                index++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        SimpleProducer simpleProducer = new SimpleProducer();
        simpleProducer.start();
    }
}
