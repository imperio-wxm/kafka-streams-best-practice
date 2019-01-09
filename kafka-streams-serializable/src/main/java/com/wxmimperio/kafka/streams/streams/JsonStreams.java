package com.wxmimperio.kafka.streams.streams;

import com.alibaba.fastjson.JSONObject;
import com.wxmimperio.kafka.streams.commons.SchemaUtils;
import com.wxmimperio.kafka.streams.serializable.AvroSerialization;
import org.apache.avro.Schema;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class JsonStreams {

    public static void main(String[] args) throws Exception {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.110:9092");
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "3000");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        //config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerialization.class);

        DateTimeFormatter eventTimePattern = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        Schema schema = SchemaUtils.getSchema("E:\\coding\\github\\kafka-streams-best-practice\\kafka-streams-serializable\\src\\main\\resources\\test.avro");
        StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, JSONObject> rawStreams = builder.stream(
                "stream-test1",
                // 指定消费时的序列化器
                //Consumed.with(Serdes.String(), new JsonSerialization())
                Consumed.with(Serdes.String(), new AvroSerialization(schema))
        );


        rawStreams.mapValues(value -> {
            if (null != value) {
                value.put("event_time", eventTimePattern.format(LocalDateTime.now()));
            }
            return value;
        }).peek((key, value) ->
                System.out.println("Key = " + key + ", value = " + value)
        );

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
    }
}
