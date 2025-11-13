package io.confluent.developer.serde;

import io.confluent.developer.proto.TradeAggregateProto;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Kafka Serializer for Protocol Buffers TradeAggregateProto.
 * Uses protobuf's built-in toByteArray() for efficient serialization.
 */
public class TradeAggregateProtoSerializer implements Serializer<TradeAggregateProto> {

    @Override
    public byte[] serialize(String topic, TradeAggregateProto tradeAggregateProto) {
        if (tradeAggregateProto == null) {
            return null;
        }
        return tradeAggregateProto.toByteArray();
    }
}
