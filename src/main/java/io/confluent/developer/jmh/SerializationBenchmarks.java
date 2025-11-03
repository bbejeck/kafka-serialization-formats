
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
import io.confluent.developer.proto.Exchange;
import io.confluent.developer.proto.StockProto;
import io.confluent.developer.serde.KryoDeserializer;
import io.confluent.developer.serde.KryoSerializer;
import io.confluent.developer.serde.SbeDeserializer;
import io.confluent.developer.serde.SbeSerializer;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.fory.Fory;
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
import java.util.concurrent.TimeUnit;


@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode({Mode.AverageTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class SerializationBenchmarks {

    public static final int BUFFER_SIZE = 1024;
    public static final double PRICE = 100.00;
    public static final int SHARES = 10_000;
    public static final String SYMBOL = "CFLT";
    public static final io.confluent.developer.Exchange EXCHANGE = io.confluent.developer.Exchange.NASDAQ;
    public static final TxnType TXN_TYPE = TxnType.BUY;


    @State(Scope.Benchmark)
    public static class JacksonState {
        ObjectMapper mapper;
        io.confluent.developer.Stock jrStock;
        byte[] jacksonBytes;
        byte[] serializationBuffer;

        @Setup(Level.Trial)
        public void setup() throws JsonProcessingException {
            mapper = new ObjectMapper();
            jrStock = new Stock(PRICE, SHARES, SYMBOL, EXCHANGE, TXN_TYPE);
            jacksonBytes = mapper.writeValueAsBytes(jrStock);
            serializationBuffer = new byte[BUFFER_SIZE];
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
            serializationBuffer = new byte[BUFFER_SIZE];
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
            stock = new Stock(100.00, 10_000, "CFLT", EXCHANGE, TXN_TYPE);
            serializedStock = kryoSerializer.serialize("topic", stock);
            serializationBuffer = new byte[BUFFER_SIZE];
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
                .setPrice(PRICE)
                .setShares(SHARES)
                .setExchange(Exchange.NASDAQ)
                .setTxn(io.confluent.developer.proto.TxnType.BUY);
        return stockBuilder.build();
    }

    @State(Scope.Benchmark)
    public static class SbeState {
        MessageHeaderEncoder messageHeaderEncoder;
        StockTradeEncoder stockTradeEncoderDirect;
        StockTradeEncoder stockTradeEncoderHeap;
        StockTradeDecoder stockTradeDecoder;
        UnsafeBuffer directUnsafeBuffer;
        UnsafeBuffer heapUnsafeBuffer;
        ByteBuffer directByteBuffer;
        ByteBuffer heapByteBuffer;
        byte[] deserializationBytes;
        SbeDeserializer sbeDeserializer;
        SbeSerializer sbeSerializer;

        @Setup(Level.Trial)
        public void setUp() {
            messageHeaderEncoder = new MessageHeaderEncoder();
            stockTradeEncoderDirect = new StockTradeEncoder();
            stockTradeEncoderHeap = new StockTradeEncoder();
            stockTradeDecoder = new StockTradeDecoder();
            sbeDeserializer = new SbeDeserializer();
            sbeSerializer = new SbeSerializer();

            directByteBuffer = ByteBuffer.allocateDirect(StockTradeEncoder.BLOCK_LENGTH + MessageHeaderEncoder.ENCODED_LENGTH);
            directUnsafeBuffer = new UnsafeBuffer(directByteBuffer);
            heapByteBuffer = ByteBuffer.allocate(StockTradeEncoder.BLOCK_LENGTH + MessageHeaderEncoder.ENCODED_LENGTH);
            heapUnsafeBuffer = new UnsafeBuffer(heapByteBuffer);

            stockTradeEncoderDirect.wrapAndApplyHeader(directUnsafeBuffer, 0, messageHeaderEncoder)
                    .price(PRICE)
                    .shares(SHARES)
                    .symbol(SYMBOL)
                    .exchange(baseline.Exchange.NASDAQ)
                    .txnType(baseline.TxnType.BUY);

            stockTradeEncoderHeap.wrapAndApplyHeader(heapUnsafeBuffer, 0, messageHeaderEncoder)
                    .price(PRICE)
                    .shares(SHARES)
                    .symbol(SYMBOL)
                    .exchange(baseline.Exchange.NASDAQ)
                    .txnType(baseline.TxnType.BUY);


            // Create deserialization bytes from the encoded data
            deserializationBytes = sbeSerializer.serialize("topic", stockTradeEncoderDirect);

        }
    }

    @State( Scope.Benchmark )
    public static class ForyState {
        Fory fory;
        Stock transaction;
        byte[] serializedTransaction;
        byte[] serializationBuffer;

        @Setup(Level.Trial)
        public void setUp() {
            fory = Fory.builder().build();
            transaction = new Stock(PRICE, SHARES, SYMBOL, io.confluent.developer.Exchange.NASDAQ, TxnType.BUY);
            fory.register(Stock.class);
            fory.register(TxnType.class);
            fory.register( io.confluent.developer.Exchange.class);

            serializedTransaction = fory.serialize(transaction);
            serializationBuffer = new byte[BUFFER_SIZE];
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
            stockTrade.setPrice(PRICE);
            stockTrade.setShares(SHARES);
            stockTrade.setSymbol(SYMBOL);
            stockTrade.setExchange(StockTradeCapnp.Exchange.NASDAQ);
            stockTrade.setTxnType(StockTradeCapnp.TxnType.BUY);

            // Pre-allocate buffers for reuse
            serializationBuffer = ByteBuffer.allocate(BUFFER_SIZE);
            ArrayOutputStream outputStream = new ArrayOutputStream(serializationBuffer);

            // Create the serialized version for deserialization tests
            Serialize.write(outputStream, messageBuilder);
            serializedCapnp = serializationBuffer.array();
            deserializationBuffer = ByteBuffer.wrap(serializedCapnp);
            serializationBuffer.clear();
        }
    }

    // ==================== Fory Benchmarks ====================

    @Benchmark
    public void measureForySerialization(ForyState furyState, Blackhole blackhole) {
        byte[] result = furyState.fory.serialize(furyState.transaction);
        blackhole.consume(result);
    }

    @Benchmark
    public void measureForyDeserialization(ForyState furyState, Blackhole blackhole) {
        Stock result = (Stock) furyState.fory.deserialize(furyState.serializedTransaction);
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

    // ==================== SBE Benchmarks ====================

    @Benchmark
    public void measureSbeSerializationDirectBuffer(SbeState state, Blackhole blackhole) {
        byte[] results = state.sbeSerializer.serialize("topic", state.stockTradeEncoderDirect);
        blackhole.consume(results);
    }

    @Benchmark
    public void measureSbeSerializationHeapBuffer(SbeState state, Blackhole blackhole) {
        byte[] results = state.sbeSerializer.serialize("topic", state.stockTradeEncoderHeap);
        blackhole.consume(results);
    }

    @Benchmark
    public void measureSbeDeserialization(SbeState state, Blackhole blackhole) {
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
        ArrayOutputStream outputStream = new ArrayOutputStream(state.serializationBuffer);
        Serialize.write(outputStream, state.messageBuilder);
        blackhole.consume(state.serializationBuffer.position());
    }

    @Benchmark
    public void measureCapnProtoDeserialization(CapnProtoState state, Blackhole blackhole) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(state.serializedCapnp);
        MessageReader messageReader = Serialize.read(buffer);
        blackhole.consume(messageReader);
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