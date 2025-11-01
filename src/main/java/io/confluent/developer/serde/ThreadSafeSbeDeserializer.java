package io.confluent.developer.serde;

import baseline.MessageHeaderDecoder;
import baseline.StockTradeDecoder;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * This class is thread safe as it creates a new decoder instance
 * with each call.
 */
public class ThreadSafeSbeDeserializer implements Deserializer<StockTradeDecoder> {

    @Override
    public StockTradeDecoder deserialize(String s, byte[] bytes) {
        MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        StockTradeDecoder stockTradeDecoder = new StockTradeDecoder();
        UnsafeBuffer unsafeBuffer = new UnsafeBuffer(bytes);
        stockTradeDecoder.wrapAndApplyHeader(unsafeBuffer, 0, messageHeaderDecoder);
        return stockTradeDecoder;
    }
}
