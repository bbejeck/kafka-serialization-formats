package io.confluent.developer.consumer;

import baseline.StockTradeDecoder;
import io.confluent.developer.Stock;
import io.confluent.developer.avro.StockAvro;
import io.confluent.developer.proto.StockProto;
import io.confluent.developer.serde.JacksonRecordDeserializer;
import io.confluent.developer.serde.SbeDeserializer;
import io.confluent.developer.util.Utils;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

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
    private static final String RECORD = "record";
    private static final String PROTO = "proto";
    private static final String AVRO = "avro";
    private static final String SBE = "sbe";


    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Usage ProducerRunner record|avro|proto|sbe numRecords");
            System.exit(1);
        }
        String messageType = args[0].toLowerCase();
        Properties props = Utils.getProperties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        int numRecords = Integer.parseInt(args[1]);
        switch (messageType) {
            case RECORD -> {
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonRecordDeserializer.class);
                props.put(ConsumerConfig.GROUP_ID_CONFIG, "record-group");
                consumeRecords(numRecords, props, "record-input");
            }
            case PROTO -> {
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
                props.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, StockProto.class);
                props.put(ConsumerConfig.GROUP_ID_CONFIG, "proto-group");
                consumeRecords(numRecords, props, "proto-input");
            }
            case SBE -> {
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SbeDeserializer.class);
                props.put(ConsumerConfig.GROUP_ID_CONFIG, "sbe-group");
                consumeRecords(numRecords, props, "sbe-input");
            }
            default -> {
                System.out.printf("Invalid message type %s%n", messageType);
                System.exit(1);
            }
        }
    }
    
    private static void consumeRecords(int numRecords, Properties props, String topic) {
        Instant startTime = Instant.now();
        int recordCount = 0;
        StringBuilder stringBuilder = new StringBuilder();
        try (Consumer<byte[], Object> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            while (recordCount < numRecords) {
                ConsumerRecords<byte[], Object> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<byte[], Object> consumerRecord : records) {
                    recordCount++;
                    switch (consumerRecord.value()) {
                       
                        case Stock jrStock -> {
                            stringBuilder.append(jrStock.symbol()).append(":@price ")
                                    .append(jrStock.price()).append(" num_shares= ")
                                    .append(jrStock.shares());
                           maybePrint(stringBuilder, recordCount);
                        }
                        case StockProto stockProto -> {
                            stringBuilder.append(stockProto.getSymbol()).append(":@price ")
                                    .append(stockProto.getPrice()).append(" num_shares= ")
                                    .append(stockProto.getShares());
                            maybePrint(stringBuilder, recordCount);
                        }
                        case StockAvro stockAvro -> {
                            stringBuilder.append(stockAvro.getSymbol()).append(":@price ")
                                    .append(stockAvro.getPrice()).append(" num_shares= ")
                                    .append(stockAvro.getShares());
                            maybePrint(stringBuilder, recordCount);
                        }
                        case StockTradeDecoder stockTradeDecoder -> {
                            stringBuilder.append(stockTradeDecoder.symbol()).append("@price ")
                                    .append(stockTradeDecoder.price()).append(" num_shares= ")
                                    .append(stockTradeDecoder.shares());
                            maybePrint(stringBuilder, recordCount);
                        }
                        
                        default -> System.out.printf("Unrecognized record: %s%n", consumerRecord.value());
                    }
                }

            }
            Instant endTime = Instant.now();
            System.out.printf("Took %d milliseconds to consume %d records%n",
                    Duration.between(startTime, endTime).toMillis(), recordCount);
        }
    }

    private static void maybePrint (StringBuilder stringBuilder, int recordCount) {
        if (recordCount % 1_000_000 == 0) {
            System.out.printf("Consumed record %s out of %d total so far%n",stringBuilder, recordCount);
        }
        stringBuilder.setLength(0);
    }
}
