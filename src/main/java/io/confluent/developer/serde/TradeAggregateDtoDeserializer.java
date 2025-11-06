package io.confluent.developer.serde;

import baseline.MessageHeaderDecoder;
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
public class TradeAggregateDtoDeserializer implements Deserializer<TradeAggregateDto> {

    private final ThreadLocal<DecoderState> decoderState = ThreadLocal.withInitial(DecoderState::new);

    private static class DecoderState {
        final TradeAggregateDecoder decoder;
        final MessageHeaderDecoder messageHeaderDecoder;

        DecoderState() {
            this.decoder = new TradeAggregateDecoder();
            this.messageHeaderDecoder = new MessageHeaderDecoder();
        }
    }

    @Override
    public TradeAggregateDto deserialize(String topic, byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        DecoderState state = decoderState.get();
        UnsafeBuffer unsafeBuffer = new UnsafeBuffer(bytes);

        state.decoder.wrapAndApplyHeader(unsafeBuffer, 0, state.messageHeaderDecoder);

        TradeAggregateDto dto = new TradeAggregateDto();
        dto.totalVolume(state.decoder.totalVolume());
        dto.volumeWeightedPrice(state.decoder.volumeWeightedPrice());
        dto.tradeCount(state.decoder.tradeCount());
        dto.minPrice(state.decoder.minPrice());
        dto.maxPrice(state.decoder.maxPrice());

        return dto;
    }
}
