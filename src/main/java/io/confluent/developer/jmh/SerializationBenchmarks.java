package io.confluent.developer.jmh;

import baseline.MessageHeaderEncoder;
import baseline.StockTradeDecoder;
import baseline.StockTradeEncoder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import io.confluent.developer.Stock;
import io.confluent.developer.TxnType;
import io.confluent.developer.avro.StockAvro;
import io.confluent.developer.proto.Exchange;
import io.confluent.developer.proto.StockProto;
import io.confluent.developer.serde.KryoDeserializer;
import io.confluent.developer.serde.KryoSerializer;
import io.confluent.developer.serde.SbeDeserializer;
import io.confluent.developer.serde.SbeSerializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.fory.Fory;
import org.apache.fory.config.Language;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * User: Bill Bejeck
 * Date: 4/25/24
 * Time: 5:13 PM
 */

@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode({Mode.AverageTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class SerializationBenchmarks {

    @State(Scope.Benchmark)
    public static class JacksonState {
        ObjectMapper mapper;
        io.confluent.developer.Stock jrSTock;
        byte[] jacksonBytes;

        @Setup(Level.Trial)
        public void setup() throws JsonProcessingException {
            mapper = new ObjectMapper();
            jrSTock = new Stock(100.00, 10_000, "CFLT", "NASDAQ", TxnType.BUY);
            jacksonBytes = mapper.writeValueAsBytes(jrSTock);
        }
    }

    @State(Scope.Benchmark)
    public static class SchemaRegistryAvroState {
        KafkaAvroSerializer avroSerializer;
        KafkaAvroDeserializer avroDeserializer;
        byte[] avroBytes;
        StockAvro stockAvro;

        @Setup(Level.Trial)
        public void setup() {
            avroSerializer = new KafkaAvroSerializer();
            avroDeserializer = new KafkaAvroDeserializer();
            stockAvro = StockAvro.newBuilder().setSymbol("CFLT")
                    .setPrice(100.00)
                    .setShares(10_000)
                    .setExchange(io.confluent.developer.avro.Exchange.NASDAQ)
                    .setType(io.confluent.developer.avro.TxnType.BUY)
                    .build();
            Map<String, Object> config = new HashMap<>();
            config.put("schema.registry.url", "mock://localhost:8081");
            avroSerializer.configure(config, false);
            config.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
            config.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_VALUE_TYPE_CONFIG, StockAvro.class.getName());
            avroDeserializer.configure(config, false);
            avroBytes = avroSerializer.serialize("topic", stockAvro);
        }

    }

    @State(Scope.Benchmark)
    public static class SchemaRegistryProtoState {
        KafkaProtobufSerializer<StockProto> protobufSerializer;
        KafkaProtobufDeserializer<StockProto> protobufDeserializer;
        byte[] serializedStock;
        StockProto stockProto;

        @Setup(Level.Trial)
        public void setup() {
            protobufSerializer = new KafkaProtobufSerializer<>();
            protobufDeserializer = new KafkaProtobufDeserializer<>();
            stockProto = stockProto();
            Map<String, Object> config = new HashMap<>();
            config.put("schema.registry.url", "mock://localhost:8081");
            protobufSerializer.configure(config, false);
            config.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, StockProto.class);
            protobufDeserializer.configure(config, false);
            serializedStock = protobufSerializer.serialize("dummy", stockProto);
        }
    }

    @State(Scope.Benchmark)
    public static class PlainProtoState {
        byte[] protoBytes;
        StockProto stockProto;

        @Setup(Level.Trial)
        public void setUp() {
            stockProto = stockProto();
            protoBytes = stockProto.toByteArray();
        }
    }

    @State(Scope.Benchmark)
    public static class KryoState {
        KryoSerializer kryoSerializer;
        KryoDeserializer kryoDeserializer;
        Stock stock;
        byte[] serializedStock;


        @Setup(Level.Trial)
        public void setUp() {
            kryoSerializer = new KryoSerializer();
            kryoDeserializer = new KryoDeserializer();
            stock = new Stock(100.00, 10_000, "CFLT", "NASDAQ", TxnType.BUY);
            serializedStock = kryoSerializer.serialize("topic", stock);
        }
    }

    private static StockProto stockProto() {
        StockProto.Builder stockBuilder = StockProto.newBuilder();
        stockBuilder.setSymbol("CFLT")
                .setPrice(100.00)
                .setShares(10_000)
                .setExchange(Exchange.NASDAQ)
                .setTxn(io.confluent.developer.proto.TxnType.BUY);
        return stockBuilder.build();
    }


    @State(Scope.Benchmark)
    public static class SbeState {
        MessageHeaderEncoder messageHeaderEncoder;
        StockTradeEncoder nonDirectStockTradeEncoder;
        StockTradeEncoder directStockTradeEncoder;
        StockTradeEncoder tenTwentyfourStockTradeEncoder;
        byte[] nonDirectBytes;
        byte[] directBytes;
        byte[] fullDirectBytes;
        Serializer<StockTradeEncoder> sbeSerializer;
        Deserializer<StockTradeDecoder> sbeDeserializer;
        ByteBufferSerializer byteBufferSerializer;

        @Setup(Level.Trial)
        public void setUp() {

            messageHeaderEncoder = new MessageHeaderEncoder();
            nonDirectStockTradeEncoder = new StockTradeEncoder();
            directStockTradeEncoder = new StockTradeEncoder();
            tenTwentyfourStockTradeEncoder = new StockTradeEncoder();
            sbeSerializer = new SbeSerializer();
            sbeDeserializer = new SbeDeserializer();
            byteBufferSerializer = new ByteBufferSerializer();

            ByteBuffer localDirectByteBuffer = ByteBuffer.allocateDirect(26);
            UnsafeBuffer localDirectUnsafeBuffer = new UnsafeBuffer(localDirectByteBuffer);

            ByteBuffer localDirect1024ByteBuffer = ByteBuffer.allocateDirect(1024);
            UnsafeBuffer localDirect1024UnsafeBuffer = new UnsafeBuffer(localDirect1024ByteBuffer);

            ByteBuffer localNonDirectBuffer = ByteBuffer.allocate(26);
            UnsafeBuffer localNonDirectUnsafeBuffer = new UnsafeBuffer(localNonDirectBuffer);

            directStockTradeEncoder.wrapAndApplyHeader(localDirectUnsafeBuffer, 0, messageHeaderEncoder)
                    .price(100.00)
                    .shares(10_000)
                    .symbol("CFLT")
                    .exchange(baseline.Exchange.NASDAQ)
                    .txnType(baseline.TxnType.BUY);
            // Serializes the underlying Direct Byte Buffer
            directBytes = sbeSerializer.serialize("topic", directStockTradeEncoder);

            nonDirectStockTradeEncoder.wrapAndApplyHeader(localNonDirectUnsafeBuffer, 0, messageHeaderEncoder)
                    .price(100.00)
                    .shares(10_000)
                    .symbol("CFLT")
                    .exchange(baseline.Exchange.NASDAQ)
                    .txnType(baseline.TxnType.BUY);
            nonDirectBytes = sbeSerializer.serialize("topic", nonDirectStockTradeEncoder);

            tenTwentyfourStockTradeEncoder.wrapAndApplyHeader(localDirect1024UnsafeBuffer, 0, messageHeaderEncoder)
                    .price(100.00)
                    .shares(10_000)
                    .symbol("CFLT")
                    .exchange(baseline.Exchange.NASDAQ)
                    .txnType(baseline.TxnType.BUY);
            fullDirectBytes = byteBufferSerializer.serialize("topic", tenTwentyfourStockTradeEncoder.buffer().byteBuffer());

        }
    }

    @State( Scope.Benchmark )
    public static class FuryState {
          Fory fury;
          Stock transaction;
          byte[] serializedTransaction;

        @Setup(Level.Trial)
        public void setUp() {
            fury = Fory.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build();
            transaction = new Stock(100.00, 10_000, "CFLT", "NASDAQ", TxnType.BUY);
            fury.register(Stock.class);
            fury.register(TxnType.class);
            serializedTransaction = fury.serialize(transaction);
        }
    }
    @Benchmark
    public byte[] measureFurySerialization(FuryState furyState) {
        return furyState.fury.serialize(furyState.transaction);
    }

    @Benchmark
    public Stock measureFuryDeserialization(FuryState furyState) {
        return (Stock) furyState.fury.deserialize(furyState.serializedTransaction);
    }

    @Benchmark
    public byte[] measureJacksonSerialization(JacksonState state) throws JsonProcessingException {
        return state.mapper.writeValueAsBytes(state.jrSTock);
    }

    @Benchmark
    public Stock measureJacksonDeserialization(JacksonState state) throws IOException {
        return state.mapper.readValue(state.jacksonBytes, Stock.class);
    }

    @Benchmark
    public byte[] measureKafkaSchemaRegistryAvroSerialization(SchemaRegistryAvroState state) {
        return state.avroSerializer.serialize("topic", state.stockAvro);
    }

    @Benchmark
    public StockAvro measureKafkaSchemaRegistryAvroDeserialization(SchemaRegistryAvroState state) {
        return (StockAvro) state.avroDeserializer.deserialize("topic", state.avroBytes);
    }

    @Benchmark
    public byte[] measureKafkaSchemaRegistryProtobufSerialization(SchemaRegistryProtoState state) {
        return state.protobufSerializer.serialize("topic", state.stockProto);
    }

    @Benchmark
    public StockProto measureKafkaSchemaRegistryProtobufDeserialization(SchemaRegistryProtoState state) {
        return state.protobufDeserializer.deserialize("topic", state.serializedStock);
    }

    @Benchmark
    public byte[] measureRawProtobufSerialization(PlainProtoState state) {
        return state.stockProto.toByteArray();
    }

    @Benchmark
    public StockProto measureRawProtobufDeserialization(PlainProtoState state) throws InvalidProtocolBufferException {
        return StockProto.parseFrom(state.protoBytes);
    }

    @Benchmark
    public byte[] measureSbeSerializationDirectByteBuffer(SbeState state) {
        return state.sbeSerializer.serialize("topic", state.directStockTradeEncoder);
    }

    @Benchmark
    public StockTradeDecoder measureSbeDeserializationDirectByteBuffer(SbeState state) {
        byte[] bytes = state.directBytes;
        return state.sbeDeserializer.deserialize("topic", bytes);
    }

    @Benchmark
    public byte[] measureSbeSerializationNonDirectByteBuffer(SbeState state) {
        return state.sbeSerializer.serialize("topic", state.nonDirectStockTradeEncoder);
    }

    @Benchmark
    public StockTradeDecoder measureSbeDeserializationNonDirectByteBuffer(SbeState state) {
        byte[] bytes = state.nonDirectBytes;
        return state.sbeDeserializer.deserialize("topic", bytes);
    }

    @Benchmark
    public byte[] measureSbeSerializationTenTwentyfourStockTradeEncoder(SbeState state) {
        ByteBuffer byteBuffer = state.tenTwentyfourStockTradeEncoder.buffer().byteBuffer();
        return state.byteBufferSerializer.serialize("topic", byteBuffer);
    }

    @Benchmark
    public StockTradeDecoder measureSbeDeserializationTenTwentyfourStockTradeDecoder(SbeState state) {
        byte[] bytes = state.fullDirectBytes;
        return state.sbeDeserializer.deserialize("topic", bytes);
    }

    @Benchmark
    public byte[] measureKryoSerialization(KryoState state) {
        return state.kryoSerializer.serialize("topic", state.stock);
    }

    @Benchmark
    public Stock measureKryoDeserialization(KryoState state) {
        return state.kryoDeserializer.deserialize("topic", state.serializedStock);
    }
    
}
