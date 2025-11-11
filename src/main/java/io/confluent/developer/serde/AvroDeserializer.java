package io.confluent.developer.serde;

import io.confluent.developer.avro.StockAvro;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class AvroDeserializer implements Deserializer<StockAvro> {


    @Override
    public StockAvro deserialize(String s, byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        try {
            DatumReader<StockAvro> datumReader = new SpecificDatumReader<>(StockAvro.class);
            BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(bytes, null);
            return datumReader.read(null, binaryDecoder);
        } catch (IOException e) {
            throw new RuntimeException("Error deserializing Avro message", e);
        }
    }
}
