package io.confluent.developer.serde;

import baseline.MessageHeaderEncoder;
import baseline.StockTradeDto;
import baseline.StockTradeEncoder;
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
public class StockTradeDtoSerializer implements Serializer<StockTradeDto> {

    private final ThreadLocal<EncoderState> encoderState = ThreadLocal.withInitial(EncoderState::new);

    private static class EncoderState {
        final ByteBuffer byteBuffer;
        final UnsafeBuffer unsafeBuffer;
        final MessageHeaderEncoder messageHeaderEncoder;
        final StockTradeEncoder encoder;
        final int headerLength;

        private EncoderState() {
            this.byteBuffer = ByteBuffer.allocateDirect(
                StockTradeEncoder.BLOCK_LENGTH + MessageHeaderEncoder.ENCODED_LENGTH
            );
            this.unsafeBuffer = new UnsafeBuffer(byteBuffer);
            this.messageHeaderEncoder = new MessageHeaderEncoder();
            this.encoder = new StockTradeEncoder();
            encoder.wrapAndApplyHeader(unsafeBuffer, 0, messageHeaderEncoder);
            this.headerLength = messageHeaderEncoder.encodedLength();
        }
    }

    @Override
    public byte[] serialize(String topic, StockTradeDto dto) {
        if (dto == null) {
            return null;
        }

        EncoderState state = encoderState.get();
        state.encoder.wrap(state.unsafeBuffer, MessageHeaderEncoder.ENCODED_LENGTH);

        state.encoder.price(dto.price());
        state.encoder.shares(dto.shares());
        state.encoder.symbol(dto.symbol());
        state.encoder.exchange(dto.exchange());
        state.encoder.txnType(dto.txnType());

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
