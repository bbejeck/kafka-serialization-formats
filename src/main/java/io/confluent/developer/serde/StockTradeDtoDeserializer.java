package io.confluent.developer.serde;

import baseline.MessageHeaderDecoder;
import baseline.StockTradeDecoder;
import baseline.StockTradeDto;
import baseline.TradeAggregateDecoder;
import baseline.TradeAggregateDto;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Kafka Deserializer for SBE TradeAggregateDto.
 * Uses SBE's DTO pattern which provides random field access via getters/setters.
 * 
 * This approach converts the flyweight decoder to a DTO for convenient field access.
 */
public class StockTradeDtoDeserializer implements Deserializer<StockTradeDto> {

    private final ThreadLocal<DecoderState> decoderState = ThreadLocal.withInitial(DecoderState::new);

    private static class DecoderState {
        final StockTradeDecoder decoder;
        final MessageHeaderDecoder messageHeaderDecoder;

        DecoderState() {
            this.decoder = new StockTradeDecoder();
            this.messageHeaderDecoder = new MessageHeaderDecoder();
        }
    }

    @Override
    public StockTradeDto deserialize(String topic, byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        DecoderState state = decoderState.get();
        UnsafeBuffer unsafeBuffer = new UnsafeBuffer(bytes);

        state.decoder.wrapAndApplyHeader(unsafeBuffer, 0, state.messageHeaderDecoder);

        StockTradeDto dto = new StockTradeDto();
        dto.price(state.decoder.price());
        dto.shares(state.decoder.shares());
        dto.symbol(state.decoder.symbol());
        dto.exchange(state.decoder.exchange());
        dto.txnType(state.decoder.txnType());

        return dto;
    }

    @Override
    public void close() {
        decoderState.remove();
    }
}
