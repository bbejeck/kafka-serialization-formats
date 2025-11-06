package io.confluent.developer.streams;

import baseline.TradeAggregateDto;
import io.confluent.developer.Stock;
import io.confluent.developer.TradeAggregate;
import io.confluent.developer.serde.*;
import io.confluent.developer.util.Utils;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Kafka Streams application for benchmarking serialization performance in stateful operations.
 * 
 * This application demonstrates how serialization format affects performance in Kafka Streams
 * state stores, where every put/get operation requires serialization/deserialization.
 * 
 * Topology: Stock trades → Session Windows → Aggregate (TradeAggregate) → Output
 * 
 * Usage: StatefulStreamsRunner <json|sbe|fury> <durationSeconds>
 */
public class StatefulStreamsRunner {

    private static final String JSON = "json";
    private static final String SBE = "sbe";
    private static final String FURY = "fury";

    private static Instant startTime;

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: StatefulStreamsRunner <json|sbe|fury> <durationSeconds>");
            System.exit(1);
        }

        String format = args[0].toLowerCase();
        int durationSeconds = Integer.parseInt(args[1]);

        Properties props = getStreamsConfig(format);
        StreamsBuilder builder = new StreamsBuilder();

        switch (format) {
            case JSON -> buildJsonTopology(builder);
            case SBE -> buildSbeTopology(builder);
            case FURY -> buildFuryTopology(builder);
            default -> {
                System.out.println("Invalid format: " + format);
                System.exit(1);
            }
        }

        runStreamsWithMetrics(builder, props, format, durationSeconds);
    }

    private static void buildJsonTopology(StreamsBuilder builder) {
        KStream<String, Stock> trades = builder.stream("json-input",
            Consumed.with(Serdes.String(), 
                Serdes.serdeFrom(new JacksonRecordSerializer(), new JacksonRecordDeserializer())));

        trades
            .selectKey((key, stock) -> stock.symbol())
            .groupByKey(Grouped.with(Serdes.String(), 
                Serdes.serdeFrom(new JacksonRecordSerializer(), new JacksonRecordDeserializer())))
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(5)))
            .aggregate(
                TradeAggregate::new,
                (key, stock, aggregate) -> {
                    aggregate.addTrade(stock.price(), stock.shares());
                    return aggregate;
                },
                (key, agg1, agg2) -> TradeAggregate.merge(agg1, agg2),
                Materialized.as("trade-aggregates-json")
            )
            .toStream()
            .to("json-output", Produced.with(
                WindowedSerdes.sessionWindowedSerdeFrom(String.class),
                Serdes.serdeFrom(new TradeAggregateJsonSerializer(), new TradeAggregateJsonDeserializer())
            ));
    }

    private static void buildSbeTopology(StreamsBuilder builder) {
        KStream<String, Stock> trades = builder.stream("sbe-input",
            Consumed.with(Serdes.String(), 
                Serdes.serdeFrom(new JacksonRecordSerializer(), new JacksonRecordDeserializer())));

        trades
            .selectKey((key, stock) -> stock.symbol())
            .groupByKey(Grouped.with(Serdes.String(), 
                Serdes.serdeFrom(new JacksonRecordSerializer(), new JacksonRecordDeserializer())))
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(5)))
            .aggregate(
                baseline.TradeAggregateDto::new,
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
                Materialized.as("trade-aggregates-sbe")
            )
            .toStream()
            .to("sbe-output", Produced.with(
                WindowedSerdes.sessionWindowedSerdeFrom(String.class),
                Serdes.serdeFrom(new TradeAggregateDtoSerializer(), new TradeAggregateDtoDeserializer())
            ));
    }

    private static void buildFuryTopology(StreamsBuilder builder) {
        KStream<String, Stock> trades = builder.stream("fory-input2",
            Consumed.with(Serdes.String(), 
                Serdes.serdeFrom(new JacksonRecordSerializer(), new JacksonRecordDeserializer())));

        trades
            .selectKey((key, stock) -> stock.symbol())
            .groupByKey(Grouped.with(Serdes.String(), 
                Serdes.serdeFrom(new JacksonRecordSerializer(), new JacksonRecordDeserializer())))
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(5)))
            .aggregate(
                TradeAggregate::new,
                (key, stock, aggregate) -> {
                    aggregate.addTrade(stock.price(), stock.shares());
                    return aggregate;
                },
                (key, agg1, agg2) -> TradeAggregate.merge(agg1, agg2),
                Materialized.as("trade-aggregates-fury")
            )
            .toStream()
            .to("fury-output", Produced.with(
                WindowedSerdes.sessionWindowedSerdeFrom(String.class),
                Serdes.serdeFrom(new TradeAggregateFurySerializer(), new TradeAggregateFuryDeserializer())
            ));
    }

    private static void runStreamsWithMetrics(StreamsBuilder builder, Properties props, 
                                              String format, int durationSeconds) throws Exception {
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        ScheduledExecutorService metricsExecutor = Executors.newSingleThreadScheduledExecutor();

        startTime = Instant.now();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close(Duration.ofSeconds(10));
            metricsExecutor.shutdown();
            latch.countDown();
        }));

        // Print metrics every 5 seconds
        metricsExecutor.scheduleAtFixedRate(() -> {
            printMetrics(streams, format);
        }, 5, 5, TimeUnit.SECONDS);

        // Automatic shutdown after duration
        metricsExecutor.schedule(() -> {
            System.out.println("\n=== FINAL METRICS ===");
            printMetrics(streams, format);
            printSummary(streams, format);
            streams.close();
            latch.countDown();
        }, durationSeconds, TimeUnit.SECONDS);

        try {
            streams.start();
            latch.await();
        } finally {
            metricsExecutor.shutdown();
        }
    }

    private static void printMetrics(KafkaStreams streams, String format) {
        Map<MetricName, ? extends Metric> metrics = streams.metrics();

        long now = System.currentTimeMillis();
        long runtimeMs = now - startTime.toEpochMilli();

        System.out.printf("\n=== [%s] Metrics at %d seconds ===%n", format.toUpperCase(), runtimeMs / 1000);

        double processRate = getMetricValue(metrics, "process-rate");
        double processLatencyAvg = getMetricValue(metrics, "process-latency-avg");
        double processLatencyMax = getMetricValue(metrics, "process-latency-max");

        // State store metrics - KEY for serialization performance
        double putRate = getMetricValue(metrics, "put-rate");
        double putLatencyAvg = getMetricValue(metrics, "put-latency-avg");
        double getRate = getMetricValue(metrics, "get-rate");
        double getLatencyAvg = getMetricValue(metrics, "get-latency-avg");

        double commitRate = getMetricValue(metrics, "commit-rate");
        double commitLatencyAvg = getMetricValue(metrics, "commit-latency-avg");

        double recordsProcessedTotal = getMetricValue(metrics, "process-total");

        System.out.printf("  Processing Rate: %.2f records/sec%n", processRate);
        System.out.printf("  Process Latency: avg=%.2fms, max=%.2fms%n", 
            processLatencyAvg, processLatencyMax);
        System.out.printf("  Total Processed: %.0f records%n", recordsProcessedTotal);
        System.out.println();

        System.out.printf("  State Store PUT: rate=%.2f ops/sec, latency=%.2fms  ← SERIALIZATION%n", 
            putRate, putLatencyAvg);
        System.out.printf("  State Store GET: rate=%.2f ops/sec, latency=%.2fms  ← DESERIALIZATION%n", 
            getRate, getLatencyAvg);
        System.out.println();

        System.out.printf("  Commit Rate: %.2f commits/sec, latency=%.2fms%n", 
            commitRate, commitLatencyAvg);

        if (runtimeMs > 0) {
            double throughput = (recordsProcessedTotal / runtimeMs) * 1000;
            System.out.printf("  Overall Throughput: %.2f records/sec%n", throughput);
        }
    }

    private static void printSummary(KafkaStreams streams, String format) {
        Map<MetricName, ? extends Metric> metrics = streams.metrics();
        long runtimeMs = System.currentTimeMillis() - startTime.toEpochMilli();

        double totalRecords = getMetricValue(metrics, "process-total");
        double avgProcessLatency = getMetricValue(metrics, "process-latency-avg");
        double avgPutLatency = getMetricValue(metrics, "put-latency-avg");
        double avgGetLatency = getMetricValue(metrics, "get-latency-avg");

        double throughput = (totalRecords / runtimeMs) * 1000;

        System.out.println("\n╔═══════════════════════════════════════════════════╗");
        System.out.printf("║ FINAL SUMMARY - %s%n", format.toUpperCase());
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
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "trade-aggregation-" + format);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0); 
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10_000);
        props.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG");

        return props;
    }
}
