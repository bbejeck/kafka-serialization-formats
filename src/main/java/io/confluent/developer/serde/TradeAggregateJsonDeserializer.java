package io.confluent.developer.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.developer.TradeAggregate;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Jackson JSON deserializer for TradeAggregate.
 * Used in Kafka Streams to demonstrate JSON deserialization performance in state stores.
 */
public class TradeAggregateJsonDeserializer implements Deserializer<TradeAggregate> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public TradeAggregate deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.readValue(data, TradeAggregate.class);
        } catch (Exception e) {
            throw new RuntimeException("Error deserializing TradeAggregate from JSON", e);
        }
    }
}
