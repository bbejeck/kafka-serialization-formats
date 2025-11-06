package io.confluent.developer;

/**
 * Aggregate value object used in Kafka Streams stateful operations.
 * This class is serialized/deserialized on every state store read/write operation.
 * 
 * Used to demonstrate serialization performance differences between formats (JSON, Proto, SBE, Fury)
 * in windowed aggregations where state store operations are frequent.
 */
public class TradeAggregate {

    private double totalVolume;
    private double volumeWeightedPrice;
    private long tradeCount;
    private double minPrice;
    private double maxPrice;

    public TradeAggregate() {
        this.totalVolume = 0.0;
        this.volumeWeightedPrice = 0.0;
        this.tradeCount = 0L;
        this.minPrice = Double.MAX_VALUE;
        this.maxPrice = Double.MIN_VALUE;
    }

    public TradeAggregate(double totalVolume, double volumeWeightedPrice, 
                         long tradeCount, double minPrice, double maxPrice) {
        this.totalVolume = totalVolume;
        this.volumeWeightedPrice = volumeWeightedPrice;
        this.tradeCount = tradeCount;
        this.minPrice = minPrice;
        this.maxPrice = maxPrice;
    }

    // Getters
    public double getTotalVolume() {
        return totalVolume;
    }

    public double getVolumeWeightedPrice() {
        return volumeWeightedPrice;
    }

    public long getTradeCount() {
        return tradeCount;
    }

    public double getMinPrice() {
        return minPrice;
    }

    public double getMaxPrice() {
        return maxPrice;
    }

    // Setters
    public void setTotalVolume(double totalVolume) {
        this.totalVolume = totalVolume;
    }

    public void setVolumeWeightedPrice(double volumeWeightedPrice) {
        this.volumeWeightedPrice = volumeWeightedPrice;
    }

    public void setTradeCount(long tradeCount) {
        this.tradeCount = tradeCount;
    }

    public void setMinPrice(double minPrice) {
        this.minPrice = minPrice;
    }

    public void setMaxPrice(double maxPrice) {
        this.maxPrice = maxPrice;
    }

    /**
     * Update this aggregate with a new trade
     */
    public void addTrade(double price, long shares) {
        this.tradeCount++;
        double tradeVolume = shares * price;
        this.totalVolume += tradeVolume;
        this.volumeWeightedPrice = 
            (this.volumeWeightedPrice * (this.tradeCount - 1) + price) / this.tradeCount;
        this.minPrice = this.tradeCount == 1 ? price : Math.min(this.minPrice, price);
        this.maxPrice = this.tradeCount == 1 ? price : Math.max(this.maxPrice, price);
    }

    /**
     * Merge another aggregate into this one (used for session window merging)
     */
    public TradeAggregate merge(TradeAggregate other) {
        if (other == null || other.tradeCount == 0) {
            return this;
        }

        long newTradeCount = this.tradeCount + other.tradeCount;
        this.totalVolume += other.totalVolume;

        // Weighted average of VWAP
        this.volumeWeightedPrice = 
            (this.volumeWeightedPrice * this.tradeCount + 
             other.volumeWeightedPrice * other.tradeCount) / newTradeCount;

        this.minPrice = Math.min(this.minPrice, other.minPrice);
        this.maxPrice = Math.max(this.maxPrice, other.maxPrice);
        this.tradeCount = newTradeCount;

        return this;
    }

    /**
     * Create a new merged aggregate from two aggregates
     */
    public static TradeAggregate merge(TradeAggregate agg1, TradeAggregate agg2) {
        if (agg1 == null) return agg2;
        if (agg2 == null) return agg1;

        TradeAggregate merged = new TradeAggregate();
        merged.tradeCount = agg1.tradeCount + agg2.tradeCount;
        merged.totalVolume = agg1.totalVolume + agg2.totalVolume;
        merged.volumeWeightedPrice = 
            (agg1.volumeWeightedPrice * agg1.tradeCount + 
             agg2.volumeWeightedPrice * agg2.tradeCount) / merged.tradeCount;
        merged.minPrice = Math.min(agg1.minPrice, agg2.minPrice);
        merged.maxPrice = Math.max(agg1.maxPrice, agg2.maxPrice);

        return merged;
    }

    @Override
    public String toString() {
        return String.format(
            "TradeAggregate{count=%d, totalVolume=%.2f, vwap=%.2f, minPrice=%.2f, maxPrice=%.2f}",
            tradeCount, totalVolume, volumeWeightedPrice, minPrice, maxPrice
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TradeAggregate that = (TradeAggregate) o;

        return Double.compare(that.totalVolume, totalVolume) == 0 &&
               Double.compare(that.volumeWeightedPrice, volumeWeightedPrice) == 0 &&
               tradeCount == that.tradeCount &&
               Double.compare(that.minPrice, minPrice) == 0 &&
               Double.compare(that.maxPrice, maxPrice) == 0;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(totalVolume);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(volumeWeightedPrice);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (int) (tradeCount ^ (tradeCount >>> 32));
        temp = Double.doubleToLongBits(minPrice);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(maxPrice);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
}
