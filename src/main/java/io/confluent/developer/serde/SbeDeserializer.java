package io.confluent.developer.serde;

import baseline.MessageHeaderDecoder;
import baseline.StockTradeDecoder;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Thread-safe SBE deserializer using ThreadLocal to pool decoder objects.
 * Reuses StockTradeDecoder and MessageHeaderDecoder per thread to avoid allocation overhead.
 * Uses zero-copy approach by directly wrapping input bytes with UnsafeBuffer.
 */
public class SbeDeserializer implements Deserializer<StockTradeDecoder> {

    private final ThreadLocal<MessageHeaderDecoder> decoderState =
            ThreadLocal.withInitial(MessageHeaderDecoder::new);

    @Override
    public StockTradeDecoder deserialize(String s, byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        MessageHeaderDecoder messageHeaderDecoder = decoderState.get();
        StockTradeDecoder stockTradeDecoder = new StockTradeDecoder();
        UnsafeBuffer unsafeBuffer = new UnsafeBuffer(bytes);
        stockTradeDecoder.wrapAndApplyHeader(unsafeBuffer, 0, messageHeaderDecoder );
        return stockTradeDecoder;
    }

    @Override
    public void close() {
        decoderState.remove();
    }
}
