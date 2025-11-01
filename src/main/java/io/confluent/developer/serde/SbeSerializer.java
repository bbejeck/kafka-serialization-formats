package io.confluent.developer.serde;

import baseline.StockTradeEncoder;
import org.apache.kafka.common.serialization.Serializer;


public class SbeSerializer implements Serializer<StockTradeEncoder> {

    @Override
    public byte[] serialize(String s, StockTradeEncoder stockTradeEncoder) {
        byte[] array = new byte[stockTradeEncoder.limit()];
        stockTradeEncoder.buffer().byteBuffer().get(array, 0, stockTradeEncoder.limit());
        return array;
    }
}
