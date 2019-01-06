package com.wxmimperio.kafka.streams.serializable;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.Charset;
import java.util.Map;

public class JsonSerialization implements Serde<JSONObject> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {

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


    static private class JSONSerializer implements Serializer<JSONObject> {
        @Override
        public void configure(Map<String, ?> map, boolean b) {

        }

        @Override
        public byte[] serialize(String s, JSONObject jsonObject) {
            if (jsonObject == null) {
                return null;
            }
            try {
                return jsonObject.toJSONString().getBytes(Charset.forName("UTF-8"));
            } catch (Exception e) {
                throw new SerializationException(String.format("Error serializing JSON message: %s", jsonObject.toJSONString()), e);
            }
        }

        @Override
        public void close() {

        }
    }

    static private class JSONDeserializer implements Deserializer<JSONObject> {
        @Override
        public void configure(Map<String, ?> map, boolean b) {

        }

        @Override
        public JSONObject deserialize(String s, byte[] bytes) {
            if (bytes == null) {
                return null;
            }
            try {
                return JSON.parseObject(new String(bytes, Charset.forName("UTF-8")));
            } catch (Exception e) {
                throw new SerializationException(String.format("Error deserialize JSON message: %s", new String(bytes, Charset.forName("UTF-8"))), e);
            }
        }

        @Override
        public void close() {

        }
    }
}
