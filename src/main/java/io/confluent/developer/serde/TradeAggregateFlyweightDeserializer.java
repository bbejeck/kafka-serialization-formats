package io.confluent.developer.serde;

import baseline.MessageHeaderDecoder;
import baseline.TradeAggregateDecoder;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Kafka Deserializer that directly uses SBE TradeAggregateDecoder flyweight.
 * This is the zero-copy approach where the decoder wraps a buffer directly.
 * 
 * Returns a decoder that wraps the original byte array - fields must be read
 * before the decoder is reused or the buffer is modified.
 */
public class TradeAggregateFlyweightDeserializer implements Deserializer<TradeAggregateDecoder> {

    private final ThreadLocal<DecoderState> decoderState = ThreadLocal.withInitial(DecoderState::new);

    private static class DecoderState {
        final MessageHeaderDecoder messageHeaderDecoder;

        DecoderState() {
            this.messageHeaderDecoder = new MessageHeaderDecoder();
        }
    }

    @Override
    public TradeAggregateDecoder deserialize(String topic, byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        DecoderState state = decoderState.get();
        UnsafeBuffer unsafeBuffer = new UnsafeBuffer(bytes);
        TradeAggregateDecoder decoder = new TradeAggregateDecoder();

        decoder.wrapAndApplyHeader(unsafeBuffer, 0, state.messageHeaderDecoder);

        return decoder;
    }

    @Override
    public void close() {
        decoderState.remove();
    }
}
