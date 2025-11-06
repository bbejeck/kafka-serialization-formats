package io.confluent.developer.consumer;

import baseline.StockTradeDecoder;
import io.confluent.developer.Stock;
import io.confluent.developer.proto.StockProto;
import io.confluent.developer.serde.ForyDeserializer;
import io.confluent.developer.serde.JacksonRecordDeserializer;
import io.confluent.developer.serde.ProtoDeserializer;
import io.confluent.developer.serde.SbeDeserializer;
import io.confluent.developer.util.Utils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;


public class ConsumerRunner {
    private static final String JSON = "record";
    private static final String PROTO = "proto";
    private static final String FORY = "fory";
    private static final String SBE = "sbe";


    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Usage ProducerRunner json|proto|fory|sbe numRecords");
            System.exit(1);
        }
        String messageType = args[0].toLowerCase();
        Properties props = Utils.getProperties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10000);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1048576);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1048576);
        Uuid uuid = Uuid.randomUuid();
        int numRecords = Integer.parseInt(args[1]);

        switch (messageType) {
            case JSON -> {
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonRecordDeserializer.class);
                props.put(ConsumerConfig.GROUP_ID_CONFIG, "json-group-"+ uuid);
                consumeRecords(numRecords, props, "json-input", "json");
            }
            case PROTO -> {
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ProtoDeserializer.class);
                props.put(ConsumerConfig.GROUP_ID_CONFIG, "proto-group-"+ uuid);
                consumeRecords(numRecords, props, "proto-input", "proto");
            }
            case SBE -> {
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SbeDeserializer.class);
                props.put(ConsumerConfig.GROUP_ID_CONFIG, "sbe-group-"+ uuid);
                consumeRecords(numRecords, props, "sbe-input", "sbe");
            }
            case FORY -> {
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ForyDeserializer.class);
                props.put(ConsumerConfig.GROUP_ID_CONFIG, "fory-group-"+ uuid);
                consumeRecords(numRecords, props, "fory-input2", "fory");
            }
            default -> {
                System.out.printf("Invalid message type %s%n", messageType);
                System.exit(1);
            }
        }
    }
    
    private static void consumeRecords(int numRecords, Properties props, String topic, String type) {
        Instant startTime = Instant.now();
        long endRun = Instant.now().plusSeconds(300).toEpochMilli();
        long recordCount = 0;
        long totalBytes = 0;
        int pollCount = 0;

        try (Consumer<byte[], Object> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            while (Instant.now().toEpochMilli() < endRun) {
                ConsumerRecords<byte[], Object> records = consumer.poll(Duration.ofMillis(5000));
                pollCount++;
                for (ConsumerRecord<byte[], Object> consumerRecord : records) {
                    Object value = consumerRecord.value();
                    switch (type) {
                        case "json" -> {
                            Stock stock = (Stock) value;
                            String symbol = stock.symbol();
                            double price = stock.price();
                            maybePrintFields(type, symbol, price, recordCount);
                        }
                        case "proto" -> {
                           StockProto protoStock = (StockProto) value;
                            String symbol = protoStock.getSymbol();
                            double price = protoStock.getPrice();
                            maybePrintFields(type, symbol, price, recordCount);
                        }
                        case "sbe" -> {
                            StockTradeDecoder sbeStock = (StockTradeDecoder) value;
                            String symbol = sbeStock.symbol();
                            double price = sbeStock.price();
                            maybePrintFields(type, symbol, price, recordCount);
                        }
                        case "fory" -> {
                            Stock furyStock = (Stock) value;
                            String symbol = furyStock.symbol();
                            double price = furyStock.price();
                            maybePrintFields(type, symbol, price, recordCount);
                        }
                    }

                    totalBytes += consumerRecord.serializedValueSize();
                    recordCount++;
                }
            }

            Instant endTime = Instant.now();
            long durationMs = Duration.between(startTime, endTime).toMillis();
            System.out.printf("Consuming records for [%s] Took %d milliseconds%n", type, durationMs);

            // Collect and print consumer metrics
            printConsumerMetrics(consumer, type, recordCount, totalBytes, durationMs, pollCount);
        }
    }

    private static void maybePrintFields(String type, String symbol, double price, long recordCount) {
        if (recordCount % 100_000 == 0) {
            System.out.printf("Fields from  %s - %s - %.2f%n", type, symbol, price);
        }
    }

    private static void printConsumerMetrics(Consumer<?, ?> consumer, String type, 
                                            long recordCount, long totalBytes,
                                            long durationMs, int pollCount) {
        Map<MetricName, ? extends Metric> metrics = consumer.metrics();

        // Extract key consumer metrics
        double fetchRate = getMetricValue(metrics, "fetch-rate");
        double fetchSizeAvg = getMetricValue(metrics, "fetch-size-avg");
        double fetchLatencyAvg = getMetricValue(metrics, "fetch-latency-avg");
        double recordsConsumedRate = getMetricValue(metrics, "records-consumed-rate");
        double bytesConsumedRate = getMetricValue(metrics, "bytes-consumed-rate");
        double recordsPerRequestAvg = getMetricValue(metrics, "records-per-request-avg");
        double recordsLagMax = getMetricValue(metrics, "records-lag-max");

        // Calculate derived metrics
        double actualThroughput = recordCount / (durationMs / 1000.0);
        double avgRecordSize = recordCount > 0 ? (double) totalBytes / recordCount : 0;
        double totalMB = totalBytes / (1024.0 * 1024.0);
        double avgRecordsPerPoll = pollCount > 0 ? (double) recordCount / pollCount : 0;

        System.out.println("\n╔═════════════════════════════════════════════════════════╗");
        System.out.printf("║ CONSUMER METRICS - %s%n", type);
        System.out.println("╠═════════════════════════════════════════════════════════╣");
        System.out.println("║ DESERIALIZATION EFFICIENCY                              ║");
        System.out.printf("║   Avg Record Size:       %.2f bytes  ← KEY METRIC%n", avgRecordSize);
        System.out.printf("║   Total Data Consumed:   %.2f MB%n", totalMB);
        System.out.printf("║   Records Consumed:      %d records%n", recordCount);
        System.out.println("╠═════════════════════════════════════════════════════════╣");
        System.out.println("║ THROUGHPUT (Higher = Better)                           ║");
        System.out.printf("║   Actual Throughput:     %.2f records/sec%n", actualThroughput);
        System.out.printf("║   Records Consumed Rate: %.2f records/sec%n", recordsConsumedRate);
        System.out.printf("║   Bytes Consumed Rate:   %.2f KB/sec%n", bytesConsumedRate / 1024.0);
        System.out.println("╠═════════════════════════════════════════════════════════╣");
        System.out.println("║ FETCH EFFICIENCY                                        ║");
        System.out.printf("║   Fetch Rate:            %.2f fetches/sec%n", fetchRate);
        System.out.printf("║   Avg Fetch Size:        %.2f bytes%n", fetchSizeAvg);
        System.out.printf("║   Fetch Latency:         %.2f ms%n", fetchLatencyAvg);
        System.out.printf("║   Records per Request:   %.2f%n", recordsPerRequestAvg);
        System.out.printf("║   Records per Poll:      %.2f%n", avgRecordsPerPoll);
        System.out.printf("║   Total Poll Calls:      %d%n", pollCount);
        if (recordsLagMax > 0) {
            System.out.printf("║   Max Records Lag:       %.0f records%n", recordsLagMax);
        }
        System.out.println("╚═════════════════════════════════════════════════════════╝\n");

        // Performance insights
        System.out.println("Performance Insights:");
        if (avgRecordsPerPoll < 100) {
            System.out.println("  ⚠ Low records per poll - consider increasing max.poll.records or fetch.min.bytes");
        }
        if (fetchLatencyAvg > 100) {
            System.out.println("  ⚠ High fetch latency - may indicate broker load or network issues");
        }
        if (actualThroughput < recordsConsumedRate * 0.8) {
            System.out.println("  ⚠ Actual throughput lower than expected - deserialization may be bottleneck");
        }
        System.out.println();
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
}
