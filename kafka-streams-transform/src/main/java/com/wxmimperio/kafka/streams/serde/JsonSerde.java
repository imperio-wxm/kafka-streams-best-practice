package com.wxmimperio.kafka.streams.serde;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class JsonSerde implements Serde<JSONObject> {

    static private class JSONSerializer implements Serializer<JSONObject> {
        /**
         * Default constructor needed by Kafka
         */
        public JSONSerializer() {
        }

        @Override
        public void configure(Map<String, ?> props, boolean isKey) {
        }

        @Override
        public byte[] serialize(String topic, JSONObject data) {
            if (data == null)
                return null;

            try {
                return data.toJSONString().getBytes("UTF-8");
            } catch (Exception e) {
                throw new SerializationException(
                        String.format("Error serializing JSON message: %s", data.toJSONString()), e);
            }
        }

        @Override
        public void close() {
        }
    }

    static private class JSONDeserializer implements Deserializer<JSONObject> {
        /**
         * Default constructor needed by Kafka
         */
        public JSONDeserializer() {
        }

        @Override
        public void configure(Map<String, ?> props, boolean isKey) {
        }

        @Override
        public JSONObject deserialize(String topic, byte[] bytes) {
            if (bytes == null)
                return null;

            try {
                return JSON.parseObject(new String(bytes, "UTF-8"));
            } catch (Exception e) {
                throw new SerializationException(
                        String.format("Error deserializing JSON message: %s", new String(bytes)), e);
            }
        }

        @Override
        public void close() {

        }
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<JSONObject> serializer() {
        return new JSONSerializer();
    }

    @Override
    public Deserializer<JSONObject> deserializer() {
        return new JSONDeserializer();
    }
}
