package io.confluent.developer.producer;

import io.confluent.developer.serde.JacksonRecordSerializer;
import io.confluent.developer.serde.SbeSerializer;
import io.confluent.developer.supplier.JavaRecordStockSupplier;
import io.confluent.developer.supplier.ProtoStockSupplier;
import io.confluent.developer.supplier.SbeRecordSupplier;
import io.confluent.developer.util.Utils;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * User: Bill Bejeck
 * Date: 4/25/24
 * Time: 4:21 PM
 */
public class ProducerRunner {

    private static final String RECORD = "record";
    private static final String PROTO = "proto";
    private static final String SBE = "sbe";

    public static void main(String[] args) {

        if (args.length == 0) {
            System.out.println("Usage ProducerRunner flatbuffer|record|proto|avro numRecords");
            System.exit(1);
        }
        String messageType = args[0].toLowerCase();
        int numRecords = Integer.parseInt(args[1]);
        Properties props = Utils.getProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        switch (messageType) {
            case RECORD -> {
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonRecordSerializer.class);
                produceRecords(numRecords, "record-input", new JavaRecordStockSupplier(), props);
            }
            case PROTO -> {
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
                props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
                produceRecords(numRecords, "proto-input", new ProtoStockSupplier(), props);
            }
            case SBE -> {
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SbeSerializer.class);
                produceRecords(numRecords, "sbe-input", new SbeRecordSupplier(), props);
            }
            default -> System.out.println("Invalid message type: " + messageType);
        }
    }

    private static <V> void produceRecords(int numRecords, String topic, Supplier<V> recordSupplier, Properties props) {
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
            Instant after = Instant.now();
            System.out.printf("Producing records Took %d milliseconds %n", after.toEpochMilli() - start.toEpochMilli());
        }
    }
}
