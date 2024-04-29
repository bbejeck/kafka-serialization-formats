package io.confluent.developer.serde;

import io.confluent.developer.flatbuffer.Stock;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;

/**
 * A Deserializer that performs no deserialization.
 * The Stock Flatbuffer class only needs a populated byte array wrapped
 * a ByteBuffer to create a new Stock instance.
 */
public class FlatBufferDeserializer implements Deserializer<Stock> {

    @Override
    public Stock deserialize(String s, byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        return Stock.getRootAsStock(ByteBuffer.wrap(bytes));
    }
}
