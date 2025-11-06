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

    private final ThreadLocal<DecoderState> decoderState = ThreadLocal.withInitial(DecoderState::new);

    private static class DecoderState {
        final StockTradeDecoder stockTradeDecoder;
        final MessageHeaderDecoder messageHeaderDecoder;

        DecoderState() {
            this.stockTradeDecoder = new StockTradeDecoder();
            this.messageHeaderDecoder = new MessageHeaderDecoder();
        }
    }

    @Override
    public StockTradeDecoder deserialize(String s, byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        DecoderState state = decoderState.get();
        UnsafeBuffer unsafeBuffer = new UnsafeBuffer(bytes);
        state.stockTradeDecoder.wrapAndApplyHeader(unsafeBuffer, 0, state.messageHeaderDecoder);
        return state.stockTradeDecoder;
    }

    @Override
    public void close() {
        decoderState.remove();
    }
}
