package io.confluent.developer.streams;

import baseline.StockTradeDto;
import baseline.TradeAggregateDto;
import io.confluent.developer.Stock;
import io.confluent.developer.TradeAggregate;
import io.confluent.developer.proto.StockProto;
import io.confluent.developer.proto.TradeAggregateProto;
import io.confluent.developer.serde.*;
import io.confluent.developer.util.Utils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.SessionStore;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Kafka Streams application for benchmarking serialization performance in stateful operations.
 * <p>
 * This application demonstrates how serialization format affects performance in Kafka Streams
 * state stores, where every put/get operation requires serialization/deserialization.
 * <p>
 * Topology: Stock trades → Session Windows → Aggregate (TradeAggregate) → Output
 * <p>
 * Usage: KafkaStreamsRunner <json|sbe|fory|proto> <durationSeconds>
 */
public class KafkaStreamsRunner {

    private static final String JSON = "json";
    private static final String SBE = "sbe";
    private static final String FORY = "fory";
    private static final String PROTO = "proto";
    private final Serde<TradeAggregateDto> tradeAggregateDtoSerde = Serdes.serdeFrom(new TradeAggregateDtoSerializer(), new TradeAggregateDtoDeserializer());
    private final Serde<StockTradeDto> stockTradeDtoSerde = Serdes.serdeFrom(new StockTradeDtoSerializer(), new StockTradeDtoDeserializer());
    private final Serde<Stock> foryStockSerde = Serdes.serdeFrom(new ForySerializer(), new ForyDeserializer());
    private final Serde<TradeAggregate> tradeAggregateForySerde = Serdes.serdeFrom(new TradeAggregateForySerializer(), new TradeAggregateForyDeserializer());
    private final Serde<TradeAggregate> tradeAggregateJsonSerde = Serdes.serdeFrom(new TradeAggregateJsonSerializer(), new TradeAggregateJsonDeserializer());
    private final Serde<Stock> stockSerde = Serdes.serdeFrom(new JacksonRecordSerializer(), new JacksonRecordDeserializer());
    private final Serde<StockProto> stockProtoSerde = Serdes.serdeFrom(new ProtoSerializer(), new ProtoDeserializer());
    private final Serde<TradeAggregateProto> tradeAggregateProtoSerde = Serdes.serdeFrom(new TradeAggregateProtoSerializer(), new TradeAggregateProtoDeserializer());
    private final Serde<String> stringSerde = Serdes.String();

    private static Instant startTime;

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: StatefulStreamsRunner <json|sbe|fory|proto> <durationSeconds>");
            System.exit(1);
        }

        String format = args[0].toLowerCase();
        int durationSeconds = Integer.parseInt(args[1]);

        Properties props = getStreamsConfig(format);
        StreamsBuilder builder = new StreamsBuilder();
        KafkaStreamsRunner streamsRunner = new KafkaStreamsRunner();

        switch (format) {
            case JSON -> streamsRunner.buildJsonTopology(builder);
            case SBE -> streamsRunner.buildSbeTopology(builder);
            case FORY -> streamsRunner.buildForyTopology(builder);
            case PROTO -> streamsRunner.buildProtoTopology(builder);
            default -> {
                System.out.println("Invalid format: " + format);
                System.exit(1);
            }
        }

        streamsRunner.runStreamsWithMetrics(builder, props, format, durationSeconds);
    }

    private void buildJsonTopology(StreamsBuilder builder) {
        KStream<String, Stock> trades = builder.stream("json-input",
                Consumed.with(stringSerde, stockSerde));

        trades
                .selectKey((key, stock) -> stock.symbol())
                .groupByKey(Grouped.with(stringSerde, stockSerde))
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(30)))
                .aggregate(
                        TradeAggregate::new,
                        (key, stock, aggregate) -> {
                            aggregate.addTrade(stock.price(), stock.shares());
                            return aggregate;
                        },
                        (key, agg1, agg2) -> TradeAggregate.merge(agg1, agg2),
                        Materialized.<String, TradeAggregate, SessionStore<Bytes, byte[]>>as("trade-aggregates-json")
                                .withKeySerde(stringSerde)
                                .withValueSerde(tradeAggregateJsonSerde)
                )
                .toStream()
                .to("json-output", Produced.with(
                        WindowedSerdes.sessionWindowedSerdeFrom(String.class), tradeAggregateJsonSerde
                ));
    }

    private void buildSbeTopology(StreamsBuilder builder) {
        KStream<String, StockTradeDto> trades = builder.stream("sbe-input",
                Consumed.with(stringSerde, stockTradeDtoSerde));

        trades
                .selectKey((key, stock) -> stock.symbol())
                .groupByKey(Grouped.with(stringSerde, stockTradeDtoSerde))
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(30)))
                .aggregate(
                        TradeAggregateDto::new,
                        (key, stock, aggregate) -> {
                            // Update DTO
                            aggregate.totalVolume(aggregate.totalVolume() + (stock.shares() * stock.price()));
                            long newCount = aggregate.tradeCount() + 1;
                            aggregate.volumeWeightedPrice(
                                    (aggregate.volumeWeightedPrice() * aggregate.tradeCount() + stock.price()) / newCount
                            );
                            aggregate.tradeCount(newCount);
                            aggregate.minPrice(newCount == 1 ? stock.price() : Math.min(aggregate.minPrice(), stock.price()));
                            aggregate.maxPrice(newCount == 1 ? stock.price() : Math.max(aggregate.maxPrice(), stock.price()));
                            return aggregate;
                        },
                        (key, agg1, agg2) -> {
                            // Merge DTOs
                            TradeAggregateDto merged = new TradeAggregateDto();
                            long totalCount = agg1.tradeCount() + agg2.tradeCount();
                            merged.tradeCount(totalCount);
                            merged.totalVolume(agg1.totalVolume() + agg2.totalVolume());
                            merged.volumeWeightedPrice(
                                    (agg1.volumeWeightedPrice() * agg1.tradeCount() +
                                            agg2.volumeWeightedPrice() * agg2.tradeCount()) / totalCount
                            );
                            merged.minPrice(Math.min(agg1.minPrice(), agg2.minPrice()));
                            merged.maxPrice(Math.max(agg1.maxPrice(), agg2.maxPrice()));
                            return merged;
                        },
                        Materialized.<String, TradeAggregateDto, SessionStore<Bytes, byte[]>>as("trade-aggregates-sbeDto")
                                .withKeySerde(stringSerde)
                                .withValueSerde(tradeAggregateDtoSerde)
                )
                .toStream()
                .to("sbe-output", Produced.with(
                        WindowedSerdes.sessionWindowedSerdeFrom(String.class),
                        tradeAggregateDtoSerde)
                );
    }

    private void buildForyTopology(StreamsBuilder builder) {
        KStream<String, Stock> trades = builder.stream("fory-input2",
                Consumed.with(stringSerde, foryStockSerde));

        trades
                .selectKey((key, stock) -> stock.symbol())
                .groupByKey(Grouped.with(stringSerde, foryStockSerde))
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(30)))
                .aggregate(
                        TradeAggregate::new,
                        (key, stock, aggregate) -> {
                            aggregate.addTrade(stock.price(), stock.shares());
                            return aggregate;
                        },
                        (key, agg1, agg2) -> TradeAggregate.merge(agg1, agg2),
                        Materialized.<String, TradeAggregate, SessionStore<Bytes, byte[]>>as("trade-aggregates-fury")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(tradeAggregateForySerde)
                )
                .toStream()
                .to("fory-output", Produced.with(
                        WindowedSerdes.sessionWindowedSerdeFrom(String.class),
                        tradeAggregateForySerde)
                );
    }

    private void buildProtoTopology(StreamsBuilder builder) {
        KStream<String, StockProto> trades = builder.stream("proto-input",
                Consumed.with(stringSerde, stockProtoSerde));

        trades
                .selectKey((key, stock) -> stock.getSymbol())
                .groupByKey(Grouped.with(stringSerde, stockProtoSerde))
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(30)))
                .aggregate(
                        () -> TradeAggregateProto.newBuilder()
                                .setTotalVolume(0.0)
                                .setVolumeWeightedPrice(0.0)
                                .setTradeCount(0)
                                .setMinPrice(Double.MAX_VALUE)
                                .setMaxPrice(Double.MIN_VALUE)
                                .build(),
                        (key, stock, aggregate) -> {
                            // Update aggregate with new trade
                            double newTotalVolume = aggregate.getTotalVolume() + (stock.getShares() * stock.getPrice());
                            long newCount = aggregate.getTradeCount() + 1;
                            double newVwap = (aggregate.getVolumeWeightedPrice() * aggregate.getTradeCount() + stock.getPrice()) / newCount;
                            double newMin = newCount == 1 ? stock.getPrice() : Math.min(aggregate.getMinPrice(), stock.getPrice());
                            double newMax = newCount == 1 ? stock.getPrice() : Math.max(aggregate.getMaxPrice(), stock.getPrice());

                            return TradeAggregateProto.newBuilder()
                                    .setTotalVolume(newTotalVolume)
                                    .setVolumeWeightedPrice(newVwap)
                                    .setTradeCount(newCount)
                                    .setMinPrice(newMin)
                                    .setMaxPrice(newMax)
                                    .build();
                        },
                        (key, agg1, agg2) -> {
                            // Merge two aggregates
                            long totalCount = agg1.getTradeCount() + agg2.getTradeCount();
                            double mergedVwap = (agg1.getVolumeWeightedPrice() * agg1.getTradeCount() +
                                    agg2.getVolumeWeightedPrice() * agg2.getTradeCount()) / totalCount;

                            return TradeAggregateProto.newBuilder()
                                    .setTotalVolume(agg1.getTotalVolume() + agg2.getTotalVolume())
                                    .setVolumeWeightedPrice(mergedVwap)
                                    .setTradeCount(totalCount)
                                    .setMinPrice(Math.min(agg1.getMinPrice(), agg2.getMinPrice()))
                                    .setMaxPrice(Math.max(agg1.getMaxPrice(), agg2.getMaxPrice()))
                                    .build();
                        },
                        Materialized.<String, TradeAggregateProto, SessionStore<Bytes, byte[]>>as("trade-aggregates-proto")
                                .withKeySerde(stringSerde)
                                .withValueSerde(tradeAggregateProtoSerde)
                )
                .toStream()
                .to("proto-output", Produced.with(
                        WindowedSerdes.sessionWindowedSerdeFrom(String.class),
                        tradeAggregateProtoSerde)
                );
    }

    private void runStreamsWithMetrics(StreamsBuilder builder, Properties props,
                                       String format, int durationSeconds) throws Exception {
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);


        startTime = Instant.now();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close(Duration.ofSeconds(10));
            latch.countDown();
        }));

        streams.start();
        latch.await(durationSeconds, TimeUnit.SECONDS);
        printMetricsSummary(streams, format);
        streams.close(Duration.ofSeconds(10));
    }

    private static void printMetricsSummary(KafkaStreams streams, String format) {
        Map<MetricName, ? extends Metric> metrics = streams.metrics();
        long runtimeMs = System.currentTimeMillis() - startTime.toEpochMilli();

        double totalRecords = getMetricValue(metrics, "stream-thread-metrics", "process-total");
        double processRate = getMetricValue(metrics, "stream-thread-metric", "process-rate");
        double avgProcessLatency = getMetricValue(metrics, "stream-thread-metrics", "process-latency-avg");
        double avgPutLatency = getMetricValue(metrics, "stream-state-metrics", "put-latency-avg");
        double avgGetLatency = getMetricValue(metrics, "stream-state-metrics", "get-latency-avg");
        double getRate = getMetricValue(metrics, "stream-state-metrics", "get-rate");
        double putRate = getMetricValue(metrics, "stream-state-metrics", "put-rate");

        System.out.println("\n╔═══════════════════════════════════════════════════╗");
        System.out.printf("║ Streams Metrics SUMMARY - %s%n", format.toUpperCase());
        System.out.println("╠═══════════════════════════════════════════════════╣");
        System.out.printf("║ Runtime:              %d seconds%n", runtimeMs / 1000);
        System.out.printf("║ Total Records:        %.0f%n", totalRecords);
        System.out.printf("║ Process Rate:         %.2f ops/sec%n", processRate);
        System.out.printf("║ Avg Process Latency:  %.2f ms%n", avgProcessLatency / 1000.0);
        System.out.printf("║ PUT Rate:             %.2f ops/sec%n", putRate);
        System.out.printf("║ Avg PUT Latency:      %.2f ms%n", avgPutLatency / 1000.0);
        System.out.printf("║ GET Rate:             %.2f ops/sec%n", getRate);
        System.out.printf("║ Avg GET Latency:      %.2f ms%n", avgGetLatency / 1000.0);
        System.out.println("╚═══════════════════════════════════════════════════╝\n");
    }

    public static double getMetricValue(Map<MetricName, ? extends Metric> metrics, String groupName, String metricName) {
        return metrics.entrySet().stream()
                .filter(entry -> entry.getKey().group().equals(groupName))
                .filter(entry -> entry.getKey().name().equals(metricName))
                .findFirst()
                .map(entry -> {
                    Object value = entry.getValue().metricValue();
                    if (value instanceof Number) {
                        return ((Number) value).doubleValue();
                    }
                    return 0.0;
                })
                .orElse(0.0);
    }

    private static Properties getStreamsConfig(String format) {
        Properties props = Utils.getProperties();
        String uuid = Uuid.randomUuid().toString();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "trade-aggregation-" + format + "-" + uuid);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10_000);
        props.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG");
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "latest");
        return props;
    }
}
