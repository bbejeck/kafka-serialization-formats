package io.confluent.developer.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.flatbuffers.FlatBufferBuilder;
import io.confluent.developer.TxnType;
import io.confluent.developer.flatbuffer.Stock;
import io.confluent.developer.util.Utils;
import net.datafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Properties;

/**
 * User: Bill Bejeck
 * Date: 4/25/24
 * Time: 4:21 PM
 */
public class ProducerRunner {
    private static final Logger LOG = LoggerFactory.getLogger(ProducerRunner.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        
        if (args.length == 0) {
            System.out.println("Usage ProducerRunner flatbuffer|record numRecords");
            System.exit(1);
        }
        String messageType = args[0];
        int numRecords = Integer.parseInt(args[1]);
        if (messageType.equals("flatbuffer")) {
             produceFlatbuffer(numRecords);
        } else if (messageType.equals("record")) {
             produceRecord(numRecords);
        } else {
            System.out.println("Invalid message type");
            System.exit(1);
        }

    }

    private static void produceFlatbuffer(int numRecords) {
        Properties props = Utils.getProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        Faker faker = new Faker();
        FlatBufferBuilder builder = new FlatBufferBuilder();
        Instant now = Instant.now();
        try(Producer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
           for (int i = 0; i < numRecords; i++) {
               Stock.startStock(builder);
               int symbolName = builder.createString(faker.stock().nsdqSymbol());
               int exchangeName = builder.createString(faker.stock().exchanges());
               int fullName = builder.createString(faker.company().name());
               int type = TxnType.values()[faker.number().numberBetween(0,2)].ordinal();
               int stock = Stock.createStock(builder, faker.number().randomDouble(2,1,200), faker.number().numberBetween(100, 10_000), symbolName, exchangeName, fullName, (byte) type);
               builder.finish(stock);
               producer.send(new ProducerRecord<>("flatbuffer-input", builder.sizedByteArray()), (metadata, exception) -> {
                   if (exception != null) {
                       LOG.error("Error producing message", exception);
                   }
               });
               builder.clear();
           }
           Instant after = Instant.now();
           System.out.printf("Producing Flatbuffer records Took %d milliseconds %n", after.toEpochMilli() - now.toEpochMilli())   ;
        }
    }

    private static void produceRecord(int numRecords) throws JsonProcessingException {
        Properties props = Utils.getProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        Faker faker = new Faker();
        Instant now = Instant.now();
        try(Producer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < numRecords; i++) {
                io.confluent.developer.Stock stock = new io.confluent.developer.Stock(faker.number().randomDouble(2,1,200),
                        faker.number().numberBetween(100, 10_000),
                        faker.stock().nsdqSymbol(),
                        faker.stock().exchanges(),
                        faker.company().name(),
                        TxnType.values()[faker.number().numberBetween(0,2)]);

                producer.send(new ProducerRecord<>("record-input",MAPPER.writeValueAsBytes(stock)), (metadata, exception) -> {
                    if (exception != null) {
                        LOG.error("Error producing message", exception);
                    }
                });
            }
            Instant after = Instant.now();
            System.out.printf("Producing Java Records with Serialization Took %d milliseconds %n", after.toEpochMilli() - now.toEpochMilli());
        }
    }
}
