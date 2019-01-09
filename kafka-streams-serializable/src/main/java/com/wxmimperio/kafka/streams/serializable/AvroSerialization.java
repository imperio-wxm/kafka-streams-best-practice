package com.wxmimperio.kafka.streams.serializable;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wxmimperio.kafka.streams.commons.SchemaUtils;
import org.apache.avro.Schema;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Map;

public class AvroSerialization implements Serde<JSONObject> {
    private static final Logger LOG = LoggerFactory.getLogger(AvroSerialization.class);

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
            if (null == data) {
                return null;
            }
            try {
                return SchemaUtils.avroSerializedValue(schema, data);
            } catch (Exception e) {
                LOG.error(String.format("Error serialize Avro message: %s", data.toJSONString()), e);
                return null;
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
            if (null == data) {
                return null;
            }
            try {
                return JSON.parseObject(SchemaUtils.readAvro(schema, data));
            } catch (Exception e) {
                LOG.error(String.format("Error deserialize Avro message: %s", new String(data, Charset.forName("UTF-8"))), e);
                return null;
            }
        }

        @Override
        public void close() {

        }
    }
}
