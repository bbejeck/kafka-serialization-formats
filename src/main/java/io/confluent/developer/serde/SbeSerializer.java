package io.confluent.developer.serde;

import baseline.StockTradeEncoder;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * User: Bill Bejeck
 * Date: 5/8/24
 * Time: 10:02 AM
 */
public class SbeSerializer implements Serializer<StockTradeEncoder> {

    @Override
    public byte[] serialize(String s, StockTradeEncoder stockTradeEncoder) {
        ByteBuffer byteBuffer = stockTradeEncoder.buffer().byteBuffer();
        byteBuffer.rewind();
        byte[] array;
        if(byteBuffer.hasArray()) {
            array  = Arrays.copyOfRange(
                    byteBuffer.array(),
                   0,
                    stockTradeEncoder.limit()
            );
        } else {
            array = new byte[stockTradeEncoder.limit()];
            byteBuffer.get(array, 0, stockTradeEncoder.limit());
        }
        return array;
    }
}
