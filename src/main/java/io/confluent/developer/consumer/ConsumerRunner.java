package io.confluent.developer.consumer;

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
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
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

        int numRecords = Integer.parseInt(args[1]);

        switch (messageType) {
            case JSON -> {
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonRecordDeserializer.class);
                props.put(ConsumerConfig.GROUP_ID_CONFIG, "json-group");
                consumeRecords(numRecords, props, "json-input", null);
            }
            case PROTO -> {
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ProtoDeserializer.class);
                props.put(ConsumerConfig.GROUP_ID_CONFIG, "proto-group");
                consumeRecords(numRecords, props, "proto-input", null);
            }
            case SBE -> {
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SbeDeserializer.class);
                props.put(ConsumerConfig.GROUP_ID_CONFIG, "sbe-group");
                consumeRecords(numRecords, props, "sbe-input", null);
            }
            case FORY -> {
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ForyDeserializer.class);
                props.put(ConsumerConfig.GROUP_ID_CONFIG, "fory-group");
                consumeRecords(numRecords, props, "fory-input", null);
            }
            default -> {
                System.out.printf("Invalid message type %s%n", messageType);
                System.exit(1);
            }
        }
    }
    
    private static void consumeRecords(int numRecords, Properties props, String topic, String type) {
        Instant startTime = Instant.now();
        int recordCount = 0;
        try (Consumer<byte[], Object> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            while (recordCount < numRecords) {
                ConsumerRecords<byte[], Object> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<byte[], Object> consumerRecord : records) {
                    consumerRecord.value();
                     recordCount++;

                    }
                }

            }
            Instant endTime = Instant.now();
            System.out.printf("Took %d milliseconds to consume %d of type %s records%n",
                    Duration.between(startTime, endTime).toMillis(), recordCount, type);
        }
}
