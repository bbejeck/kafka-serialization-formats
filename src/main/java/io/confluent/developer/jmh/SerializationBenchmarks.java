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
import io.confluent.developer.serde.SbeDeserializer;
import io.confluent.developer.serde.SbeSerializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

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
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class SerializationBenchmarks {

    @State(Scope.Benchmark)
    public static class JacksonState {
        public ObjectMapper mapper;
        public io.confluent.developer.Stock jrSTock;
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
        KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer();
        KafkaAvroDeserializer avroDeserializer = new KafkaAvroDeserializer();
        byte[] avroBytes;
        StockAvro stockAvro;

        @Setup(Level.Trial)
        public void setup(){
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
        KafkaProtobufSerializer<StockProto> protobufSerializer = new KafkaProtobufSerializer<>();
        KafkaProtobufDeserializer<StockProto> protobufDeserializer = new KafkaProtobufDeserializer<>();
        byte[] serializedStock;
        StockProto stockProto;

        @Setup(Level.Trial)
        public void setup() {
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
        MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
        StockTradeEncoder nonDirectStockTradeEncoder = new StockTradeEncoder();
        StockTradeEncoder directStockTradeEncoder = new StockTradeEncoder();
        StockTradeEncoder tenTwentyfourStockTradeEncoder = new StockTradeEncoder();
        byte[] nonDirectBytes;
        byte[] directBytes;
        byte[] fullDirectBytes;
        Serializer<StockTradeEncoder> sbeSerializer = new SbeSerializer();
        Deserializer<StockTradeDecoder> sbeDeserializer = new SbeDeserializer();
        ByteBufferSerializer byteBufferSerializer = new ByteBufferSerializer();

        @Setup(Level.Trial)
        public void setUp() {
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

    @Benchmark
    public void measureJacksonSerialization(JacksonState state, Blackhole bh) throws JsonProcessingException {
        bh.consume(state.mapper.writeValueAsBytes(state.jrSTock));
    }

    @Benchmark
    public void measureJacksonDeserialization(JacksonState state, Blackhole bh) throws IOException {
        Stock stock = state.mapper.readValue(state.jacksonBytes, Stock.class);
        bh.consume(stock);
    }

    @Benchmark
    public void measureKafkaSchemaRegistryAvroSerialization(SchemaRegistryAvroState state, Blackhole bh) {
        byte[] avroBytes = state.avroSerializer.serialize("topic", state.stockAvro);
        bh.consume(avroBytes);
    }

    @Benchmark
    public void measureKafkaSchemaRegistryAvroDeserialization(SchemaRegistryAvroState state, Blackhole bh) {
        StockAvro stockAvro = (StockAvro) state.avroDeserializer.deserialize("topic", state.avroBytes);
        bh.consume(stockAvro);
    }

    @Benchmark
    public void measureKafkaSchemaRegistryProtobufSerialization(SchemaRegistryProtoState state, Blackhole bh) {
        byte[] protoBytes = state.protobufSerializer.serialize("topic", state.stockProto);
        bh.consume(protoBytes);
    }

    @Benchmark
    public void measureKafkaSchemaRegistryProtobufDeserialization(SchemaRegistryProtoState state, Blackhole bh) {
        StockProto stockProto = state.protobufDeserializer.deserialize("topic", state.serializedStock);
        bh.consume(stockProto);
    }

    @Benchmark
    public void measureRawProtobufSerialization(PlainProtoState state, Blackhole bh)  {
        byte[] protoBytes = state.stockProto.toByteArray();
        bh.consume(protoBytes);
    }

    @Benchmark
    public void measureRawProtobufDeserialization(PlainProtoState state, Blackhole bh) throws InvalidProtocolBufferException {
        StockProto stockProto = StockProto.parseFrom(state.protoBytes);
        bh.consume(stockProto);
    }

    @Benchmark
    public void measureSbeSerializationDirectByteBuffer(SbeState state, Blackhole bh) {
        byte[] bytes = state.sbeSerializer.serialize("topic", state.directStockTradeEncoder);
        bh.consume(bytes);
    }

    @Benchmark
    public void measureSbeDeserializationDirectByteBuffer(SbeState state, Blackhole bh) {
        byte[] bytes = state.directBytes;
        StockTradeDecoder stockTradeDecoder =  state.sbeDeserializer.deserialize("topic", bytes);
        bh.consume(stockTradeDecoder);
    }

    @Benchmark
    public void measureSbeSerializationNonDirectByteBuffer(SbeState state, Blackhole bh) {
        byte[] bytes = state.sbeSerializer.serialize("topic", state.nonDirectStockTradeEncoder);
        bh.consume(bytes);
    }

    @Benchmark
    public void measureSbeDeserializationNonDirectByteBuffer(SbeState state, Blackhole bh) {
        byte[] bytes = state.nonDirectBytes;
        StockTradeDecoder stockTradeDecoder = state.sbeDeserializer.deserialize("topic", bytes);
        bh.consume(stockTradeDecoder);
    }

    @Benchmark
    public void measureSbeSerializationTenTwentyfourStockTradeEncoder(SbeState state, Blackhole bh) {
        ByteBuffer byteBuffer =  state.tenTwentyfourStockTradeEncoder.buffer().byteBuffer();
        byte[] bytes = state.byteBufferSerializer.serialize("topic", byteBuffer);
        bh.consume(bytes);
    }

   @Benchmark
    public void measureSbeDeserializationTenTwentyfourStockTradeDecoder(SbeState state, Blackhole bh) {
        byte[] bytes = state.fullDirectBytes;
       StockTradeDecoder stockTradeDecoder = state.sbeDeserializer.deserialize("topic", bytes);
       bh.consume(stockTradeDecoder);
   }

    

}
