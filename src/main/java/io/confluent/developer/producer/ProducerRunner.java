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
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class ProducerRunner {

    private static final String JSON = "json";
    private static final String PROTO = "proto";
    private static final String SBE = "sbe";
    private static final String FORY = "fory";


    public static void main(String[] args) {

        if (args.length == 0) {
            System.out.println("Usage ProducerRunner json|proto|sbe|fory numRecords");
            System.exit(1);
        }
        String messageType = args[0].toLowerCase();
        int numRecords = Integer.parseInt(args[1]);
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
                produceRecords(numRecords, "FORY", "fory-input", new JavaRecordStockSupplier(), props);
            }
            default -> System.out.println("Invalid message type: " + messageType);
        }
    }

    private static <V> void produceRecords(int numRecords, String type, String topic, Supplier<V> recordSupplier, Properties props) {
        Instant start = Instant.now();
        AtomicInteger counter = new AtomicInteger(1);
        try (Producer<byte[], V> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < numRecords; i++) {
                producer.send(new ProducerRecord<>(topic, recordSupplier.get()), (metadata, exception) -> {
                    if (exception != null) {
                        System.out.printf("Error producing message %s%n", exception);
                    }
                    if (counter.getAndIncrement() % 1_000_000 == 0) {
                        System.out.printf("Produced %d records%n", counter.get());
                    }
                });
            }
            producer.flush();
            Instant after = Instant.now();
            System.out.printf("Producing records for [%s] Took %d milliseconds %n", type, after.toEpochMilli() - start.toEpochMilli());
        }
    }
}
