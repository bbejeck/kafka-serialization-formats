package io.confluent.developer.serde;

import baseline.MessageHeaderEncoder;
import baseline.TradeAggregateEncoder;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;

/**
 * Kafka Serializer that directly uses SBE TradeAggregateEncoder flyweight.
 * This is the zero-copy approach where the encoder wraps a buffer directly.
 * <p>
 * More efficient than the DTO approach but requires sequential field access.
 */
public class TradeAggregateFlyweightSerializer implements Serializer<TradeAggregateEncoder> {

    private final ThreadLocal<EncoderState> encoderState = ThreadLocal.withInitial(EncoderState::new);

    private static class EncoderState {
        final ByteBuffer byteBuffer;
        final UnsafeBuffer unsafeBuffer;
        final MessageHeaderEncoder messageHeaderEncoder;

        private EncoderState() {
            this.byteBuffer = ByteBuffer.allocate(
                TradeAggregateEncoder.BLOCK_LENGTH + MessageHeaderEncoder.ENCODED_LENGTH
            );
            this.unsafeBuffer = new UnsafeBuffer(byteBuffer);
            this.messageHeaderEncoder = new MessageHeaderEncoder();
        }
    }

    @Override
    public byte[] serialize(String topic, TradeAggregateEncoder encoder) {
        if (encoder == null) {
            return null;
        }

        // The encoder already contains the encoded data
        int length = encoder.limit();
        byte[] result = new byte[length];
        encoder.buffer().getBytes(0, result, 0, length);

        return result;
    }

    @Override
    public void close() {
        encoderState.remove();
    }
}
