package io.confluent.developer.serde;

import com.google.protobuf.InvalidProtocolBufferException;
import io.confluent.developer.proto.StockProto;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * User: Bill Bejeck
 * Date: 10/27/25
 * Time: 4:22 PM
 */
public class ProtoDeserializer implements Deserializer<StockProto>{

    @Override
    public StockProto deserialize(String s, byte[] bytes) {
        try {
            return StockProto.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }
}
