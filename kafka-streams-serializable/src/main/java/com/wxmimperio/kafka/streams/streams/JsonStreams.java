package com.wxmimperio.kafka.streams.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.nio.charset.Charset;
import java.util.Properties;

public class JsonStreams {

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.110:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, byte[]> rawStreams = builder.stream("stream-test1");
        rawStreams.filter((key,value) -> {
            System.out.println(new String(value, Charset.forName("UTF-8")));
            return true;
        }).peek((key,value) -> System.out.println("Key = " + key + ", value = " + new String(value, Charset.forName("UTF-8"))));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

    }
}
