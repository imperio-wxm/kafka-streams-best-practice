package com.wxmimperio.kafka.streams;

import com.wxmimperio.kafka.streams.topology.TopologyBuilder;
import com.wxmimperio.kafka.streams.topology.TransformTopology;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KafkaStreamsTransform {
    private static Logger LOG = LoggerFactory.getLogger(KafkaStreamsTransform.class);
    private static KafkaStreams streams;
    private static final CountDownLatch latch = new CountDownLatch(1);

    public static void main(String[] args) {

        String source = "src_test_wxm_split,src_test_wxm_split001";
        String sink = "test_wxm_split,test_wxm_split001";

        TopologyBuilder builder = new TransformTopology();
        Properties customizedConfig = Optional.ofNullable(builder.config()).orElse(new Properties());
        Topology topology = builder.get(source, sink);
        streams = new KafkaStreams(topology, getStreamsConfig(customizedConfig));
        streams.start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static StreamsConfig getStreamsConfig(Properties customizedConfig) {
        Properties settings = new Properties();
        // stream config
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "TransformStreams");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.110:9092");
        // consumer config
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        settings.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 60000);
        settings.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        // producer config
        settings.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        // per-topology customized config
        settings.putAll(customizedConfig);
        StreamsConfig config = new StreamsConfig(settings);
        LOG.info("config = " + settings.toString());
        return config;
    }
}
