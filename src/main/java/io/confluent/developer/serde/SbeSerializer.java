package io.confluent.developer.serde;

import baseline.StockTradeEncoder;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;

/**
 * User: Bill Bejeck
 * Date: 5/8/24                                                 Ø
 * Time: 10:02 AM
 */
public class SbeSerializer implements Serializer<StockTradeEncoder> {

    @Override
    public byte[] serialize(String s, StockTradeEncoder stockTradeEncoder) {
        ByteBuffer byteBuffer = stockTradeEncoder.buffer().byteBuffer();
        byte[] array;
        if (byteBuffer.hasArray() && byteBuffer.array().length == stockTradeEncoder.limit()) {
            return byteBuffer.array();
        }
        byteBuffer.rewind();
        array = new byte[stockTradeEncoder.limit()];
        byteBuffer.get(array, 0, stockTradeEncoder.limit());
        return array;
    }
}
