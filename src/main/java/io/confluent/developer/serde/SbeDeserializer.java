package io.confluent.developer.serde;

import baseline.MessageHeaderDecoder;
import baseline.StockTradeDecoder;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.kafka.common.serialization.Deserializer;


public class SbeDeserializer implements Deserializer<StockTradeDecoder> {

    @Override
    public StockTradeDecoder deserialize(String s, byte[] bytes) {
        StockTradeDecoder stockTradeDecoder = new StockTradeDecoder();
        MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        UnsafeBuffer unsafeBuffer = new UnsafeBuffer(bytes);
        stockTradeDecoder.wrapAndApplyHeader(unsafeBuffer, 0, messageHeaderDecoder);
        return stockTradeDecoder;
    }
}
