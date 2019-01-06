package com.wxmimperio.kafka.streams.commons;

import com.alibaba.fastjson.JSONObject;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.*;
import java.util.Map;
import java.util.Set;

public class SchemaUtils {

    public static Schema getSchema(String path) throws Exception {
        return new Schema.Parser().parse(new FileInputStream(new File(path)));
    }

    public static String readAvro(Schema schema, byte[] values) throws IOException {
        DatumReader<GenericRecord> datumReader = new SpecificDatumReader<GenericRecord>(schema);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(values);
        Decoder decoder = DecoderFactory.get().binaryDecoder(byteArrayInputStream, null);
        GenericRecord gr = null;
        gr = datumReader.read(gr, decoder);
        return gr.toString();
    }

    public static byte[] avroSerializedValue(Schema schema, JSONObject jsonObject) throws IOException {
        DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<GenericRecord>(schema);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        datumWriter.write(generateAvroByJson(schema,jsonObject), encoder);
        encoder.flush();
        out.close();
        return out.toByteArray();
    }

    private static GenericRecord generateAvroByJson(Schema schema, JSONObject jsonObject) {
        GenericRecord genericRecord = new GenericData.Record(schema);
        for(Map.Entry<String, Object> data :  jsonObject.entrySet()) {
            genericRecord.put(data.getKey(), data.getValue());
        }
        return genericRecord;
    }
}
