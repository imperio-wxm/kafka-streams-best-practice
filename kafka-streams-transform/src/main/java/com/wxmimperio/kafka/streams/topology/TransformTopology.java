package com.wxmimperio.kafka.streams.topology;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.sdo.dw.rtc.cleaning.Cleaner;
import com.sdo.dw.rtc.cleaning.Result;
import com.wxmimperio.kafka.streams.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLDecoder;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TransformTopology implements TopologyBuilder {
    private static Logger LOG = LoggerFactory.getLogger(TransformTopology.class);

    private Cleaner cleaner;

    @Override
    public Topology get(String sources, String sinks) {
        List<String> sinkList = Arrays.stream(sinks.split(",", -1)).map(String::trim).collect(Collectors.toList());
        JSONObject config = new JSONObject();
        String configString = "{\"decoder\":{\"type\":\"json\"},\"filters\":[{\"type\":\"extractjson\",\"params\":{\"nested_field\":\"data.event_time\",\"new_field\":\"event_time\"}}]}";
        try {
            String configStr = URLDecoder.decode(configString, "utf-8");
            config = JSON.parseObject(configStr);
        } catch (Exception e) {
            LOG.error("failed to parse config.", e);
        }
        final String configStr = config.toJSONString();
        try {
            cleaner = Cleaner.create(config);
        } catch (Exception e) {
            LOG.error("failed to generate cleaner", e);
        }

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, JSONObject> rawStream = builder.stream(Stream.of(sources.split(",", -1)).map(String::trim).distinct().collect(Collectors.toList()));

        rawStream.flatMap((KeyValueMapper<String, JSONObject, Iterable<? extends KeyValue<?, ?>>>) (key, value) -> {
            List<KeyValue<String, JSONObject>> list = Lists.newArrayList();
            String raw = value.toJSONString();
            Result result = cleaner.process(raw);
            if (result.isSuccessful()) {
                for (JSONObject payload : result.getPayload()) {
                    if (payload.isEmpty()) {
                        continue;
                    }
                    if (payload.containsKey("_key")) {
                        key = payload.getString("_key");
                    }
                    payload.put("_rawdata", raw);
                    list.add(new KeyValue<>(key, payload));
                }
            } else {
                LOG.error(MessageFormat.format(
                        "Failed to process! config = {0},  source_data = {1}, upstream = {2}",
                        configStr, result.getSource(), result.getUpstream()), result.getThrowable()
                );
            }
            return list;
        }).to((key, value, recordContext) -> {
            String sinkTopic = recordContext.topic().substring(4);
            if (!sinkList.contains(sinkTopic)) {
                LOG.error(String.format("Can not sink msg = %s to sink = %s", value, sinkTopic));
                return null;
            } else {
                return sinkTopic;
            }
        });
        return builder.build();
    }

    @Override
    public Properties config() {
        Properties props = new Properties();
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "2");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
        return props;
    }
}
