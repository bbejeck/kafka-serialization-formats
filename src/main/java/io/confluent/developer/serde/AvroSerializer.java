package io.confluent.developer.serde;

import io.confluent.developer.avro.StockAvro;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AvroSerializer implements Serializer<StockAvro> {


    @Override
    public byte[] serialize(String s, StockAvro data) {
        if (data == null) {
            return null;
        }
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            DatumWriter<StockAvro> datumWriter = new SpecificDatumWriter<>(data.getSchema());
            BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            datumWriter.write(data, binaryEncoder);
            binaryEncoder.flush();
            return outputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Error serializing Avro message", e);
        }
    }
}
