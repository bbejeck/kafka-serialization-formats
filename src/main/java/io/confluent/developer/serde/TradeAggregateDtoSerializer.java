package io.confluent.developer.serde;

import baseline.MessageHeaderEncoder;
import baseline.TradeAggregateDto;
import baseline.TradeAggregateEncoder;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;

/**
 * Kafka Serializer for SBE TradeAggregateDto.
 * Uses SBE's DTO pattern which provides random field access via getters/setters.
 * 
 * This approach converts the DTO to the flyweight encoder for serialization,
 * combining the convenience of DTOs with SBE's efficient encoding.
 */
public class TradeAggregateDtoSerializer implements Serializer<TradeAggregateDto> {

    private final ThreadLocal<EncoderState> encoderState = ThreadLocal.withInitial(EncoderState::new);

    private static class EncoderState {
        final ByteBuffer byteBuffer;
        final UnsafeBuffer unsafeBuffer;
        final MessageHeaderEncoder messageHeaderEncoder;
        final TradeAggregateEncoder encoder;

        private EncoderState() {
            this.byteBuffer = ByteBuffer.allocate(
                TradeAggregateEncoder.BLOCK_LENGTH + MessageHeaderEncoder.ENCODED_LENGTH
            );
            this.unsafeBuffer = new UnsafeBuffer(byteBuffer);
            this.messageHeaderEncoder = new MessageHeaderEncoder();
            this.encoder = new TradeAggregateEncoder();
        }
    }

    @Override
    public byte[] serialize(String topic, TradeAggregateDto dto) {
        if (dto == null) {
            return null;
        }

        EncoderState state = encoderState.get();
        state.byteBuffer.clear();

        state.encoder.wrapAndApplyHeader(state.unsafeBuffer, 0, state.messageHeaderEncoder);

        state.encoder.totalVolume(dto.totalVolume());
        state.encoder.volumeWeightedPrice(dto.volumeWeightedPrice());
        state.encoder.tradeCount(dto.tradeCount());
        state.encoder.minPrice(dto.minPrice());
        state.encoder.maxPrice(dto.maxPrice());

        int length = state.encoder.limit();
        byte[] result = new byte[length];
        state.unsafeBuffer.getBytes(0, result, 0, length);

        return result;
    }

    @Override
    public void close() {
        encoderState.remove();
    }
}
