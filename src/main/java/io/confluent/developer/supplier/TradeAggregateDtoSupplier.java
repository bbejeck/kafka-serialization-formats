package io.confluent.developer.supplier;

import baseline.TradeAggregateDto;

import java.util.function.Supplier;

/**
 * Supplier for SBE TradeAggregateDto instances.
 * Generates realistic trade aggregate data for benchmarking.
 * 
 * DTOs provide random field access and mutable state, making them
 * convenient for application code at the cost of object allocation.
 */
public class TradeAggregateDtoSupplier implements Supplier<TradeAggregateDto> {

    @Override
    public TradeAggregateDto get() {
        // Generate realistic trade aggregate data
        double totalVolume = 1_000_000.0 + (Math.random() * 5_000_000.0);
        double vwap = 100.0 + (Math.random() * 500.0);
        long tradeCount = 100 + (long)(Math.random() * 1000);
        double minPrice = vwap - (Math.random() * 50.0);
        double maxPrice = vwap + (Math.random() * 50.0);

        // Create and populate DTO using fluent setters
        TradeAggregateDto dto = new TradeAggregateDto();
        dto.totalVolume(totalVolume);
        dto.volumeWeightedPrice(vwap);
        dto.tradeCount(tradeCount);
        dto.minPrice(minPrice);
        dto.maxPrice(maxPrice);

        return dto;
    }
}
