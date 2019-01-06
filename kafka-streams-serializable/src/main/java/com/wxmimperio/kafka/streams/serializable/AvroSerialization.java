package com.wxmimperio.kafka.streams.serializable;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class AvroSerialization  implements Serde<JSONObject> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        
    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<JSONObject> serializer() {
        return null;
    }

    @Override
    public Deserializer<JSONObject> deserializer() {
        return null;
    }
}
