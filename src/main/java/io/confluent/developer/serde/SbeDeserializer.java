package io.confluent.developer.serde;

import baseline.MessageHeaderDecoder;
import baseline.StockTradeDecoder;
import baseline.StockTradeEncoder;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;

/**
 * User: Bill Bejeck
 * Date: 5/8/24
 * Time: 10:18 AM
 */
public class SbeDeserializer implements Deserializer<StockTradeDecoder> {

    @Override
    public StockTradeDecoder deserialize(String s, byte[] bytes) {

        ByteBuffer byteBuffer = ByteBuffer.allocate(bytes.length);
        UnsafeBuffer unsafeBuffer = new UnsafeBuffer(byteBuffer);
        MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        StockTradeDecoder stockTradeDecoder = new StockTradeDecoder();

        byteBuffer.put(bytes);
        stockTradeDecoder.wrapAndApplyHeader(unsafeBuffer, 0, messageHeaderDecoder);
        return stockTradeDecoder;
    }
}
