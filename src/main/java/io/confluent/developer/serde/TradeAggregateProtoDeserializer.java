package io.confluent.developer.serde;

import com.google.protobuf.InvalidProtocolBufferException;
import io.confluent.developer.proto.TradeAggregateProto;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Kafka Deserializer for Protocol Buffers TradeAggregateProto.
 * Uses protobuf's built-in parseFrom() for efficient deserialization.
 */
public class TradeAggregateProtoDeserializer implements Deserializer<TradeAggregateProto> {

    @Override
    public TradeAggregateProto deserialize(String topic, byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        try {
            return TradeAggregateProto.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Failed to deserialize TradeAggregateProto", e);
        }
    }
}
