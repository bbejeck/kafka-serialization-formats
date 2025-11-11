package io.confluent.developer.serde;

import io.confluent.developer.TradeAggregate;
import org.apache.fory.Fory;
import org.apache.fory.ThreadSafeFory;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Fury serializer for TradeAggregate.
 * Used in Kafka Streams to demonstrate Fury serialization performance in state stores.
 */
public class TradeAggregateForySerializer implements Serializer<TradeAggregate> {

    private final ThreadSafeFory fury;

    public TradeAggregateForySerializer() {
        this.fury = Fory.builder().buildThreadSafeFory();
        fury.register(TradeAggregate.class);
    }

    @Override
    public byte[] serialize(String topic, TradeAggregate data) {
        if (data == null) {
            return null;
        }
        return fury.serialize(data);
    }
}
