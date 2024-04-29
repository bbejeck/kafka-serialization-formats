package io.confluent.developer.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.developer.Stock;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

/**
 * User: Bill Bejeck
 * Date: 4/26/24
 * Time: 1:00 PM
 */
public class JacksonRecordDeserializer implements Deserializer<Stock> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Stock deserialize(String s, byte[] bytes) {
        try {
            return objectMapper.readValue(bytes, Stock.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
