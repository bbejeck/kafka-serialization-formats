package io.confluent.developer.streams;

import baseline.StockTradeDto;
import baseline.TradeAggregateDto;
import io.confluent.developer.Stock;
import io.confluent.developer.TradeAggregate;
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
 * Usage: StatefulStreamsRunner <json|sbe|fury> <durationSeconds>
 */
public class StatefulStreamsRunner {

    private static final String JSON = "json";
    private static final String SBE = "sbe";
    private static final String FORY = "fory";
    private final Serde<TradeAggregateDto> tradeAggregateDtoSerde = Serdes.serdeFrom(new TradeAggregateDtoSerializer(), new TradeAggregateDtoDeserializer());
    private final Serde<StockTradeDto> stockTradeDtoSerde = Serdes.serdeFrom(new StockTradeDtoSerializer(), new StockTradeDtoDeserializer());
    private final Serde<Stock> foryStockSerde = Serdes.serdeFrom(new ForySerializer(), new ForyDeserializer());
    private final Serde<TradeAggregate> tradeAggregateForySerde = Serdes.serdeFrom(new TradeAggregateForySerializer(), new TradeAggregateForyDeserializer());
    private final Serde<TradeAggregate> tradeAggregateJsonSerde = Serdes.serdeFrom(new TradeAggregateJsonSerializer(), new TradeAggregateJsonDeserializer());
    private final Serde<Stock> stockSerde = Serdes.serdeFrom(new JacksonRecordSerializer(), new JacksonRecordDeserializer());
    private final Serde<String> stringSerde = Serdes.String();

    private static Instant startTime;

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: StatefulStreamsRunner <json|sbe|fory> <durationSeconds>");
            System.exit(1);
        }

        String format = args[0].toLowerCase();
        int durationSeconds = Integer.parseInt(args[1]);

        Properties props = getStreamsConfig(format);
        StreamsBuilder builder = new StreamsBuilder();
        StatefulStreamsRunner runner = new StatefulStreamsRunner();

        switch (format) {
            case JSON -> runner.buildJsonTopology(builder);
            case SBE -> runner.buildSbeTopology(builder);
            case FORY -> runner.buildForyTopology(builder);
            default -> {
                System.out.println("Invalid format: " + format);
                System.exit(1);
            }
        }

        runner.runStreamsWithMetrics(builder, props, format, durationSeconds);
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

        double totalRecords = getMetricValue(metrics, "process-total");
        double avgProcessLatency = getMetricValue(metrics, "process-latency-avg");
        double avgPutLatency = getMetricValue(metrics, "put-latency-avg");
        double avgGetLatency = getMetricValue(metrics, "get-latency-avg");

        double throughput = (totalRecords / runtimeMs) * 1000;

        System.out.println("\n╔═══════════════════════════════════════════════════╗");
        System.out.printf("║ Streams Metrics SUMMARY - %s%n", format.toUpperCase());
        System.out.println("╠═══════════════════════════════════════════════════╣");
        System.out.printf("║ Runtime:              %d seconds%n", runtimeMs / 1000);
        System.out.printf("║ Total Records:        %.0f%n", totalRecords);
        System.out.printf("║ Throughput:           %.2f records/sec%n", throughput);
        System.out.printf("║ Avg Process Latency:  %.2f ms%n", avgProcessLatency);
        System.out.printf("║ Avg PUT Latency:      %.2f ms  ← SERIALIZATION%n", avgPutLatency);
        System.out.printf("║ Avg GET Latency:      %.2f ms  ← DESERIALIZATION%n", avgGetLatency);
        System.out.println("╚═══════════════════════════════════════════════════╝\n");
    }

    private static double getMetricValue(Map<MetricName, ? extends Metric> metrics, String metricName) {
        return metrics.entrySet().stream()
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
