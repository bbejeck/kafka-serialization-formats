package io.confluent.developer.serde;

import io.confluent.developer.proto.StockProto;
import org.apache.kafka.common.serialization.Serializer;


public class ProtoSerializer implements Serializer<StockProto> {

    @Override
    public byte[] serialize(String s, StockProto stockProto) {
        return stockProto.toByteArray();
    }
}
