package io.confluent.developer.producer;

import io.confluent.developer.serde.ForySerializer;
import io.confluent.developer.serde.JacksonRecordSerializer;
import io.confluent.developer.serde.ProtoSerializer;
import io.confluent.developer.serde.SbeSerializer;
import io.confluent.developer.supplier.JavaRecordStockSupplier;
import io.confluent.developer.supplier.ProtoStockSupplier;
import io.confluent.developer.supplier.SbeRecordSupplier;
import io.confluent.developer.util.Utils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class ProducerRunner {

    private static final String JSON = "json";
    private static final String PROTO = "proto";
    private static final String SBE = "sbe";
    private static final String FORY = "fory";
    private static final int SECONDS_TO_RUN = 600;


    public static void main(String[] args) throws InterruptedException{

        if (args.length == 0) {
            System.out.println("Usage ProducerRunner json|proto|sbe|fory");
            System.exit(1);
        }
        String messageType = args[0].toLowerCase();
        int numRecords = 1_000_000;
        Properties props = Utils.getProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1048576);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);
        switch (messageType) {
            case JSON -> {
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonRecordSerializer.class);
                produceRecords(numRecords, "JSON", "json-input", new JavaRecordStockSupplier(), props);
            }
            case PROTO -> {
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProtoSerializer.class);
                produceRecords(numRecords, "PROTO", "proto-input", new ProtoStockSupplier(), props);
            }
            case SBE -> {
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SbeSerializer.class);
                produceRecords(numRecords, "SBE", "sbe-input", new SbeRecordSupplier(), props);
            }
            case FORY -> {
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ForySerializer.class);
                produceRecords(numRecords, "FORY", "fory-input2", new JavaRecordStockSupplier(), props);
            }
            default -> System.out.println("Invalid message type: " + messageType);
        }
    }

    private static <V> void produceRecords(int numRecords, String type, String topic, Supplier<V> recordSupplier, Properties props) throws InterruptedException{
        long endRun = Instant.now().plusSeconds(SECONDS_TO_RUN).toEpochMilli();
        Instant start = Instant.now();
        AtomicInteger counter = new AtomicInteger(1);
        try (Producer<byte[], V> producer = new KafkaProducer<>(props)) {
            while (Instant.now().toEpochMilli() < endRun) {
                List<V> records = new ArrayList<>();
                for (int i = 0; i < numRecords; i++) {
                    records.add(recordSupplier.get());
                }
                for (V record : records) {
                    producer.send(new ProducerRecord<>(topic, record), (metadata, exception) -> {
                        if (exception != null) {
                            System.out.printf("Error producing message %s%n", exception);
                        }
                        if (counter.getAndIncrement() % 100_000 == 0) {
                            System.out.printf("Produced %d records%n", counter.get());
                        }
                    });
                }
                producer.flush();
                Thread.sleep(250);
            }
            Instant after = Instant.now();
            long durationMs = after.toEpochMilli() - start.toEpochMilli();
            printProducerMetrics(producer, type, numRecords, durationMs);
        }
    }

    private static void printProducerMetrics(Producer<?, ?> producer, String type, int numRecords, long durationMs) {
        Map<MetricName, ? extends Metric> metrics = producer.metrics();

        // Extract key metrics
        double recordSendRate = getMetricValue(metrics, "record-send-rate");
        double byteRate = getMetricValue(metrics, "outgoing-byte-rate");
        double recordSizeAvg = getMetricValue(metrics, "record-size-avg");
        double recordSizeMax = getMetricValue(metrics, "record-size-max");
        double batchSizeAvg = getMetricValue(metrics, "batch-size-avg");
        double recordsPerRequestAvg = getMetricValue(metrics, "records-per-request-avg");
        double requestLatencyAvg = getMetricValue(metrics, "request-latency-avg");
        double compressionRate = getMetricValue(metrics, "compression-rate-avg");

        // Calculate derived metrics
        double actualThroughput = numRecords / (durationMs / 1000.0);
        double totalMB = (recordSizeAvg * numRecords) / (1024.0 * 1024.0);

        System.out.println("\n╔═════════════════════════════════════════════════════════╗");
        System.out.printf("║ PRODUCER METRICS - %s%n", type);
        System.out.println("╠═════════════════════════════════════════════════════════╣");
        System.out.println("║ SERIALIZATION EFFICIENCY (Lower = Better)              ║");
        System.out.printf("║   Avg Record Size:       %.2f bytes  ← KEY METRIC%n", recordSizeAvg);
        System.out.printf("║   Max Record Size:       %.2f bytes%n", recordSizeMax);
        System.out.printf("║   Total Data Size:       %.2f MB%n", totalMB);
        if (compressionRate > 0) {
            System.out.printf("║   Compression Ratio:     %.2f%n", compressionRate);
        }
        System.out.println("╠═════════════════════════════════════════════════════════╣");
        System.out.println("║ THROUGHPUT (Higher = Better)                           ║");
        System.out.printf("║   Record Send Rate:      %.2f records/sec%n", recordSendRate);
        System.out.printf("║   Actual Throughput:     %.2f records/sec%n", actualThroughput);
        System.out.printf("║   Byte Rate:             %.2f KB/sec%n", byteRate / 1024.0);
        System.out.println("╠═════════════════════════════════════════════════════════╣");
        System.out.println("║ BATCHING EFFICIENCY                                     ║");
        System.out.printf("║   Avg Batch Size:        %.2f bytes%n", batchSizeAvg);
        System.out.printf("║   Records per Request:   %.2f%n", recordsPerRequestAvg);
        System.out.printf("║   Request Latency:       %.2f ms%n", requestLatencyAvg);
        System.out.println("╚═════════════════════════════════════════════════════════╝\n");
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
