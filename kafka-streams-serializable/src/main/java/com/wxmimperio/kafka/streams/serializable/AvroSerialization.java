package com.wxmimperio.kafka.streams.serializable;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wxmimperio.kafka.streams.commons.SchemaUtils;
import org.apache.avro.Schema;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

public class AvroSerialization  implements Serde<JSONObject> {

    private Schema schema;

    public AvroSerialization(Schema schema) {
        this.schema = schema;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<JSONObject> serializer() {
        return new AvroSerializer(schema);
    }

    @Override
    public Deserializer<JSONObject> deserializer() {
        return new AvroDeserializer(schema);
    }

    static private class AvroSerializer implements Serializer<JSONObject> {
        private Schema schema;

        public AvroSerializer(Schema schema) {
            this.schema = schema;
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {

        }

        @Override
        public byte[] serialize(String topic, JSONObject data) {
            if(null == data) {
                return null;
            }
            try {
                return SchemaUtils.avroSerializedValue(schema, data);
            } catch (IOException e) {
                throw new SerializationException(String.format("Error serialize Avro message: %s", data.toJSONString()), e);
            }
        }

        @Override
        public void close() {

        }
    }

    static private class AvroDeserializer implements Deserializer<JSONObject> {

        private Schema schema;

        public AvroDeserializer(Schema schema) {
            this.schema = schema;
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {

        }

        @Override
        public JSONObject deserialize(String topic, byte[] data) {
            if(null == data) {
                return null;
            }
            try {
                return JSON.parseObject(SchemaUtils.readAvro(schema, data));
            } catch (IOException e) {
                throw new SerializationException(String.format("Error deserialize Avro message: %s", new String(data, Charset.forName("UTF-8"))), e);
            }
        }

        @Override
        public void close() {

        }
    }
}
