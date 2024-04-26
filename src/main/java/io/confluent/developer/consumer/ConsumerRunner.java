package io.confluent.developer.consumer;

import io.confluent.developer.Stock;
import io.confluent.developer.util.Utils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;

/**
 * User: Bill Bejeck
 * Date: 4/26/24
 * Time: 12:42 PM
 */
public class ConsumerRunner {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerRunner.class);
    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Usage ProducerRunner flatbuffer|record numRecords");
            System.exit(1);
        }
        String messageType = args[0];
        Properties props = Utils.getProperties();
        int numRecords = Integer.parseInt(args[1]);
        if (messageType.equals("flatbuffer")) {
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "flatbuffer-group3");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumeFlatbuffer(numRecords, props, "flatbuffer-input");
        } else if (messageType.equals("record")) {
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonRecordDeserializer.class);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "record-group");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumeRecord(numRecords, props, "record-input");
        } else {
            System.out.println("Invalid message type");
            System.exit(1);
        }
    }


    private static void consumeFlatbuffer(int numRecords, Properties props, String topic) {
        Instant consumeFlatbufferStart = Instant.now();
        try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            StringBuilder stringBuilder = new StringBuilder();
            consumer.subscribe(Collections.singletonList(topic));
            int recordCount = 0;
            while (recordCount < numRecords) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<byte[], byte[]> flatbufferRecord : records) {
                    ByteBuffer byteBuffer = ByteBuffer.wrap(flatbufferRecord.value());
                    io.confluent.developer.flatbuffer.Stock stock = io.confluent.developer.flatbuffer.Stock.getRootAsStock(byteBuffer);
                    stringBuilder.append(stock.symbol()).append(" : ")
                            .append(stock.price()).append(", ")
                            .append(stock.shares());
                    if (recordCount % 1_000 == 0) {
                        System.out.printf("Flatbuffer stock record %s%n", stringBuilder);
                    }
                    stringBuilder.setLength(0);
                    recordCount++;
                }
            }
            Duration duration = Duration.between(consumeFlatbufferStart, Instant.now());
            System.out.println("Consumed " + recordCount + " Flatbuffer records in " + duration.toMillis() + " milliseconds");
        }
    }

    private static void consumeRecord(int numRecords, Properties props, String topic) {
        Instant recordConsumeStart = Instant.now();
        try (Consumer<byte[], Stock> consumer = new KafkaConsumer<>(props)) {
            StringBuilder stringBuilder = new StringBuilder();
            consumer.subscribe(Collections.singletonList(topic));
            int recordCount = 0;
            while (recordCount < numRecords) {
                ConsumerRecords<byte[], Stock> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<byte[], Stock> stockRecord : records) {
                    Stock stock = stockRecord.value();
                    stringBuilder.append(stock.symbol()).append(" : ")
                            .append(stock.price()).append(", ")
                            .append(stock.shares());
                    if (recordCount % 10_000 == 0) {
                        LOG.info("Java Record stock {}", stringBuilder);
                    }
                    stringBuilder.setLength(0);
                    recordCount++;
                }
            }
            Duration duration = Duration.between(recordConsumeStart, Instant.now());
            System.out.println("Consumed " + recordCount + " Stock records in " + duration.toMillis() + " milliseconds");
        }
    }

}
