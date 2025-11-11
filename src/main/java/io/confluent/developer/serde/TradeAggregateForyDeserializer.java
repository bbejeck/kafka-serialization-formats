package io.confluent.developer.serde;

import io.confluent.developer.TradeAggregate;
import org.apache.fory.Fory;
import org.apache.fory.ThreadSafeFory;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Fury deserializer for TradeAggregate.
 * Used in Kafka Streams to demonstrate Fory deserialization performance in state stores.
 */
public class TradeAggregateForyDeserializer implements Deserializer<TradeAggregate> {

    private final ThreadSafeFory fury;

    public TradeAggregateForyDeserializer() {
        this.fury = Fory.builder().buildThreadSafeFory();
        fury.register(TradeAggregate.class);
    }

    @Override
    public TradeAggregate deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        return (TradeAggregate) fury.deserialize(data);
    }
}
