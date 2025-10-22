
package io.confluent.developer.jmh;

import baseline.MessageHeaderEncoder;
import baseline.StockTradeDecoder;
import baseline.StockTradeEncoder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import io.confluent.developer.Stock;
import io.confluent.developer.StockTradeCapnp;
import io.confluent.developer.TxnType;
import io.confluent.developer.avro.StockAvro;
import io.confluent.developer.proto.Exchange;
import io.confluent.developer.proto.StockProto;
import io.confluent.developer.serde.KryoDeserializer;
import io.confluent.developer.serde.KryoSerializer;
import io.confluent.developer.serde.SbeDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.fory.Fory;
import org.apache.fory.config.Language;
import org.capnproto.ArrayOutputStream;
import org.capnproto.MessageBuilder;
import org.capnproto.MessageReader;
import org.capnproto.Serialize;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.AsyncProfiler;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * User: Bill Bejeck
 * Date: 4/25/24
 * Time: 5:13 PM
 */

@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 3)
@BenchmarkMode({Mode.AverageTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class SerializationBenchmarks {

    @State(Scope.Benchmark)
    public static class JacksonState {
        ObjectMapper mapper;
        io.confluent.developer.Stock jrStock;
        byte[] jacksonBytes;
        // Reusable buffer for serialization
        byte[] serializationBuffer;

        @Setup(Level.Trial)
        public void setup() throws JsonProcessingException {
            mapper = new ObjectMapper();
            jrStock = new Stock(100.00, 10_000, "CFLT", "NASDAQ", TxnType.BUY);
            jacksonBytes = mapper.writeValueAsBytes(jrStock);
            // Pre-allocate buffer large enough for serialized data
            serializationBuffer = new byte[1024];
        }
    }

    @State(Scope.Benchmark)
    public static class SchemaRegistryAvroState {
        KafkaAvroSerializer avroSerializer;
        KafkaAvroDeserializer avroDeserializer;
        byte[] avroBytes;
        StockAvro stockAvro;
        // Reusable buffer for deserialized objects
        byte[] serializationBuffer;

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
            serializationBuffer = new byte[1024];
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            if (avroSerializer != null) avroSerializer.close();
            if (avroDeserializer != null) avroDeserializer.close();
        }
    }

    @State(Scope.Benchmark)
    public static class SchemaRegistryProtoState {
        KafkaProtobufSerializer<StockProto> protobufSerializer;
        KafkaProtobufDeserializer<StockProto> protobufDeserializer;
        byte[] serializedStock;
        StockProto stockProto;
        byte[] serializationBuffer;

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
            serializedStock = protobufSerializer.serialize("topic", stockProto);
            protobufDeserializer.deserialize("topic", serializedStock);
            serializationBuffer = new byte[1024];
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            if (protobufSerializer != null) protobufSerializer.close();
            if (protobufDeserializer != null) protobufDeserializer.close();
        }
    }

    @State(Scope.Benchmark)
    public static class PlainProtoState {
        byte[] protoBytes;
        StockProto stockProto;
        byte[] serializationBuffer;

        @Setup(Level.Trial)
        public void setUp() {
            stockProto = stockProto();
            protoBytes = stockProto.toByteArray();
            serializationBuffer = new byte[1024];
        }
    }

    @State(Scope.Benchmark)
    public static class KryoState {
        KryoSerializer kryoSerializer;
        KryoDeserializer kryoDeserializer;
        Stock stock;
        byte[] serializedStock;
        byte[] serializationBuffer;

        @Setup(Level.Trial)
        public void setUp() {
            kryoSerializer = new KryoSerializer();
            kryoDeserializer = new KryoDeserializer();
            stock = new Stock(100.00, 10_000, "CFLT", "NASDAQ", TxnType.BUY);
            serializedStock = kryoSerializer.serialize("topic", stock);
            serializationBuffer = new byte[1024];
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            if (kryoSerializer != null) kryoSerializer.close();
            if (kryoDeserializer != null) kryoDeserializer.close();
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
        StockTradeEncoder stockTradeEncoder;
        StockTradeDecoder stockTradeDecoder;
        UnsafeBuffer directUnsafeBuffer;
        UnsafeBuffer nonDirectUnsafeBuffer;
        UnsafeBuffer direct1024UnsafeBuffer;
        ByteBuffer directByteBuffer;
        ByteBuffer nonDirectByteBuffer;
        ByteBuffer direct1024ByteBuffer;
        byte[] deserializationBytes;
        SbeDeserializer sbeDeserializer;

        @Setup(Level.Trial)
        public void setUp() {
            messageHeaderEncoder = new MessageHeaderEncoder();
            stockTradeEncoder = new StockTradeEncoder();
            stockTradeDecoder = new StockTradeDecoder();
            sbeDeserializer = new SbeDeserializer();

            // Pre-allocate all buffers
            directByteBuffer = ByteBuffer.allocateDirect(256);
            directUnsafeBuffer = new UnsafeBuffer(directByteBuffer);

            direct1024ByteBuffer = ByteBuffer.allocateDirect(1024);
            direct1024UnsafeBuffer = new UnsafeBuffer(direct1024ByteBuffer);

            nonDirectByteBuffer = ByteBuffer.allocate(256);
            nonDirectUnsafeBuffer = new UnsafeBuffer(nonDirectByteBuffer);

            // Pre-encode data into the buffers (this is setup, not measured)
            stockTradeEncoder.wrapAndApplyHeader(directUnsafeBuffer, 0, messageHeaderEncoder)
                    .price(100.00)
                    .shares(10_000)
                    .symbol("CFLT")
                    .exchange(baseline.Exchange.NASDAQ)
                    .txnType(baseline.TxnType.BUY);

            // Create deserialization bytes from the encoded data
            deserializationBytes = new byte[stockTradeEncoder.encodedLength()];
            directUnsafeBuffer.getBytes(0, deserializationBytes);
        }
    }

    @State( Scope.Benchmark )
    public static class FuryState {
        Fory fury;
        Stock transaction;
        byte[] serializedTransaction;
        byte[] serializationBuffer;

        @Setup(Level.Trial)
        public void setUp() {
            fury = Fory.builder().build();
            transaction = new Stock(100.00, 10_000, "CFLT", "NASDAQ", TxnType.BUY);
            fury.register(Stock.class);
            fury.register(TxnType.class);
            serializedTransaction = fury.serialize(transaction);
            serializationBuffer = new byte[1024];
        }
    }

    @State(Scope.Benchmark)
    public static class CapnProtoState {
        MessageBuilder messageBuilder;
        StockTradeCapnp.StockTrade.Builder stockTrade;
        byte[] serializedCapnp;
        ByteBuffer serializationBuffer;
        ByteBuffer deserializationBuffer;

        @Setup(Level.Trial)
        public void setUp() throws IOException {
            messageBuilder = new MessageBuilder();
            stockTrade = messageBuilder.initRoot(StockTradeCapnp.StockTrade.factory);

            // Initialize the stock trade data once
            stockTrade.setPrice(100.00);
            stockTrade.setShares(10_000);
            stockTrade.setSymbol("CFLT");
            stockTrade.setExchange(StockTradeCapnp.Exchange.NASDAQ);
            stockTrade.setTxnType(StockTradeCapnp.TxnType.BUY);

            // Pre-allocate buffers for reuse
            serializationBuffer = ByteBuffer.allocate(1024 * 2);
            ArrayOutputStream outputStream = new ArrayOutputStream(serializationBuffer);

            // Create the serialized version for deserialization tests
            Serialize.write(outputStream, messageBuilder);
            serializedCapnp = new byte[serializationBuffer.position()];
            serializationBuffer.flip();
            serializationBuffer.get(serializedCapnp);
            serializationBuffer.clear();

            deserializationBuffer = ByteBuffer.wrap(serializedCapnp);
        }
    }

    // ==================== Fury Benchmarks ====================

    @Benchmark
    public void measureFurySerialization(FuryState furyState, Blackhole blackhole) {
        byte[] result = furyState.fury.serialize(furyState.transaction);
        blackhole.consume(result);
    }

    @Benchmark
    public void measureFuryDeserialization(FuryState furyState, Blackhole blackhole) {
        Stock result = (Stock) furyState.fury.deserialize(furyState.serializedTransaction);
        blackhole.consume(result);
    }

    // ==================== Jackson Benchmarks ====================

    @Benchmark
    public void measureJacksonSerialization(JacksonState state, Blackhole blackhole) throws JsonProcessingException {
        byte[] result = state.mapper.writeValueAsBytes(state.jrStock);
        blackhole.consume(result);
    }

    @Benchmark
    public void measureJacksonDeserialization(JacksonState state, Blackhole blackhole) throws IOException {
        Stock result = state.mapper.readValue(state.jacksonBytes, Stock.class);
        blackhole.consume(result);
    }

    // ==================== Kafka Schema Registry Avro Benchmarks ====================

    @Benchmark
    public void measureKafkaSchemaRegistryAvroSerialization(SchemaRegistryAvroState state, Blackhole blackhole) {
        byte[] result = state.avroSerializer.serialize("topic", state.stockAvro);
        blackhole.consume(result);
    }

    @Benchmark
    public void measureKafkaSchemaRegistryAvroDeserialization(SchemaRegistryAvroState state, Blackhole blackhole) {
        StockAvro result = (StockAvro) state.avroDeserializer.deserialize("topic", state.avroBytes);
        blackhole.consume(result);
    }

    // ==================== Kafka Schema Registry Protobuf Benchmarks ====================

    @Benchmark
    public void measureKafkaSchemaRegistryProtobufSerialization(SchemaRegistryProtoState state, Blackhole blackhole) {
        byte[] result = state.protobufSerializer.serialize("topic", state.stockProto);
        blackhole.consume(result);
    }

    @Benchmark
    public void measureKafkaSchemaRegistryProtobufDeserialization(SchemaRegistryProtoState state, Blackhole blackhole) {
        StockProto result = state.protobufDeserializer.deserialize("topic", state.serializedStock);
        blackhole.consume(result);
    }

    // ==================== Raw Protobuf Benchmarks ====================

    @Benchmark
    public void measureRawProtobufSerialization(PlainProtoState state, Blackhole blackhole) {
        byte[] result = state.stockProto.toByteArray();
        blackhole.consume(result);
    }

    @Benchmark
    public void measureRawProtobufDeserialization(PlainProtoState state, Blackhole blackhole) throws InvalidProtocolBufferException {
        StockProto result = StockProto.parseFrom(state.protoBytes);
        blackhole.consume(result);
    }

    // ==================== SBE Direct ByteBuffer Benchmarks ====================

    @Benchmark
    public void measureSbeSerializationDirectByteBuffer(SbeState state, Blackhole blackhole) {
        // Reset buffer position
        state.directByteBuffer.clear();

        // Encode directly into the reused buffer
        state.stockTradeEncoder.wrapAndApplyHeader(state.directUnsafeBuffer, 0, state.messageHeaderEncoder)
                .price(100.00)
                .shares(10_000)
                .symbol("CFLT")
                .exchange(baseline.Exchange.NASDAQ)
                .txnType(baseline.TxnType.BUY);

        // Consume the encoded length to prevent dead code elimination
        blackhole.consume(state.stockTradeEncoder.encodedLength());
    }

    @Benchmark
    public void measureSbeDeserializationDirectByteBuffer(SbeState state, Blackhole blackhole) {
        StockTradeDecoder result = state.sbeDeserializer.deserialize("topic", state.deserializationBytes);
        blackhole.consume(result);
    }

    // ==================== SBE Non-Direct ByteBuffer Benchmarks ====================

    @Benchmark
    public void measureSbeSerializationNonDirectByteBuffer(SbeState state, Blackhole blackhole) {
        // Reset buffer position
        state.nonDirectByteBuffer.clear();

        // Encode directly into the reused buffer
        state.stockTradeEncoder.wrapAndApplyHeader(state.nonDirectUnsafeBuffer, 0, state.messageHeaderEncoder)
                .price(100.00)
                .shares(10_000)
                .symbol("CFLT")
                .exchange(baseline.Exchange.NASDAQ)
                .txnType(baseline.TxnType.BUY);

        blackhole.consume(state.stockTradeEncoder.encodedLength());
    }

    @Benchmark
    public void measureSbeDeserializationNonDirectByteBuffer(SbeState state, Blackhole blackhole) {
        StockTradeDecoder result = state.sbeDeserializer.deserialize("topic", state.deserializationBytes);
        blackhole.consume(result);
    }

    // ==================== SBE 1024 ByteBuffer Benchmarks ====================

    @Benchmark
    public void measureSbeSerializationOneThousandTwentyFourByteBuffer(SbeState state, Blackhole blackhole) {
        // Reset buffer position
        state.direct1024ByteBuffer.clear();

        // Encode directly into the reused buffer
        state.stockTradeEncoder.wrapAndApplyHeader(state.direct1024UnsafeBuffer, 0, state.messageHeaderEncoder)
                .price(100.00)
                .shares(10_000)
                .symbol("CFLT")
                .exchange(baseline.Exchange.NASDAQ)
                .txnType(baseline.TxnType.BUY);

        blackhole.consume(state.stockTradeEncoder.encodedLength());
    }

    @Benchmark
    public void measureSbeDeserializationOneThousandTwentyFourByteBuffer(SbeState state, Blackhole blackhole) {
        StockTradeDecoder result = state.sbeDeserializer.deserialize("topic", state.deserializationBytes);
        blackhole.consume(result);
    }

    // ==================== Kryo Benchmarks ====================

    @Benchmark
    public void measureKryoSerialization(KryoState state, Blackhole blackhole) {
        byte[] result = state.kryoSerializer.serialize("topic", state.stock);
        blackhole.consume(result);
    }

    @Benchmark
    public void measureKryoDeserialization(KryoState state, Blackhole blackhole) {
        Stock result = state.kryoDeserializer.deserialize("topic", state.serializedStock);
        blackhole.consume(result);
    }

    // ==================== Cap'n Proto Benchmarks ====================

    @Benchmark
    public void measureCapnProtoSerialization(CapnProtoState state, Blackhole blackhole) throws IOException {
        state.serializationBuffer.clear();
        ArrayOutputStream outputStream = new ArrayOutputStream(state.serializationBuffer);
        Serialize.write(outputStream, state.messageBuilder);
        blackhole.consume(state.serializationBuffer.position());
    }

    @Benchmark
    public void measureCapnProtoDeserialization(CapnProtoState state, Blackhole blackhole) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(state.serializedCapnp);
        MessageReader messageReader = Serialize.read(buffer);
        StockTradeCapnp.StockTrade.Reader result = messageReader.getRoot(StockTradeCapnp.StockTrade.factory);
        blackhole.consume(result);
    }


    public static void main(String[] args) {
        try {
            // Check if profiling is enabled via command-line argument
            boolean enableProfiling = args.length > 0 && "true".equalsIgnoreCase(args[0]);

            ChainedOptionsBuilder optBuilder = new OptionsBuilder()
                    .include(SerializationBenchmarks.class.getSimpleName())
                    .result("benchmark-results.json")
                    .resultFormat(ResultFormatType.JSON);

            // Conditionally add async-profiler
            if (enableProfiling) {
                System.out.println("✓ async-profiler ENABLED - flame graphs will be generated");
                optBuilder.addProfiler(AsyncProfiler.class, "output=flamegraph;simple=true");
            } else {
                System.out.println("✗ async-profiler DISABLED - run with argument 'true' to enable profiling");
            }

            Options opt = optBuilder.build();
            new Runner(opt).run();
        } catch (RunnerException e) {
            throw new RuntimeException(e);
        }
    }

}