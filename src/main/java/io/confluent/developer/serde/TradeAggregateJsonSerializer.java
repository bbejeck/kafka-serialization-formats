package io.confluent.developer.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.developer.TradeAggregate;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Jackson JSON serializer for TradeAggregate.
 * Used in Kafka Streams to demonstrate JSON serialization performance in state stores.
 */
public class TradeAggregateJsonSerializer implements Serializer<TradeAggregate> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, TradeAggregate data) {
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException("Error serializing TradeAggregate to JSON", e);
        }
    }
}
