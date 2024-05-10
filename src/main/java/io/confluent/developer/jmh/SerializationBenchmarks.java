package io.confluent.developer.jmh;

import baseline.MessageHeaderDecoder;
import baseline.MessageHeaderEncoder;
import baseline.StockTradeDecoder;
import baseline.StockTradeEncoder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.developer.Stock;
import io.confluent.developer.TxnType;
import io.confluent.developer.proto.Exchange;
import io.confluent.developer.proto.StockProto;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.agrona.concurrent.UnsafeBuffer;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
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
@Measurement(iterations = 10)
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
    public static class ProtoState {
        KafkaProtobufSerializer<StockProto> protobufSerializer = new KafkaProtobufSerializer<>();
        KafkaProtobufDeserializer<StockProto> protobufDeserializer = new KafkaProtobufDeserializer<>();
        byte[] serializedStock;
        StockProto stockProto;

        @Setup(Level.Trial)
        public void setup() {
            StockProto.Builder stockBuilder = StockProto.newBuilder();
            stockBuilder.setSymbol("CFLT")
                    .setPrice(100.00)
                    .setShares(10_000)
                    .setExchange(Exchange.NASDAQ)
                    .setTxn(io.confluent.developer.proto.TxnType.BUY);
            Map<String, Object> config = new HashMap<>();
            config.put("schema.registry.url", "mock://localhost:8081");
            protobufSerializer.configure(config, false);
            protobufDeserializer.configure(config, false);
            stockProto = stockBuilder.build();
            serializedStock = protobufSerializer.serialize("dummy", stockProto);
        }
    }

    @State(Scope.Benchmark)
    public static class SbeState {
        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
        UnsafeBuffer unsafeBuffer = new UnsafeBuffer(byteBuffer);
        MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
        StockTradeEncoder stockTradeEncoder = new StockTradeEncoder();
        ByteBuffer decodeBuffer;
        UnsafeBuffer decodeUnsafeBuffer = new UnsafeBuffer();
        MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        StockTradeDecoder stockTradeDecoder = new StockTradeDecoder();
        byte[] bytes = new byte[26];

        @Setup(Level.Trial)
        public void setUp() {
            stockTradeEncoder.wrapAndApplyHeader(unsafeBuffer, 0, messageHeaderEncoder)
                    .price(100.00f)
                    .shares(10_000)
                    .symbol("CFLT")
                    .exchange(baseline.Exchange.NASDAQ)
                    .txnType(baseline.TxnType.BUY);

            bytes = Arrays.copyOfRange(
                    byteBuffer.array(),
                    0,
                    stockTradeEncoder.limit());

        }
    }

    @Benchmark
    public void measureJacksonToByteArray(JacksonState state, Blackhole bh) throws JsonProcessingException {
        bh.consume(state.mapper.writeValueAsBytes(state.jrSTock));
    }

    @Benchmark
    public void measureBytesToObjectJacksonMapper(JacksonState state, Blackhole bh) throws IOException {
        Stock stock = state.mapper.readValue(state.jacksonBytes, Stock.class);
        bh.consume(stock);
    }

    @Benchmark
    public void measureKafkaProtobufSerializerToByteArray(ProtoState state, Blackhole bh) {
        byte[] protoBytes = state.protobufSerializer.serialize("topic", state.stockProto);
        bh.consume(protoBytes);
    }

    @Benchmark
    public void measureKafkaProtobufDeserializerToByteArray(ProtoState state, Blackhole bh) {
        StockProto stockProto = state.protobufDeserializer.deserialize("topic", state.serializedStock);
        bh.consume(stockProto);
    }

    @Benchmark
    public void measureRawProtobufToByteArray(ProtoState state, Blackhole bh)  {
        byte[] protoBytes = state.stockProto.toByteArray();
        bh.consume(protoBytes);
    }

    @Benchmark
    public void measureSbeSerialization(SbeState state, Blackhole bh) {
        ByteBuffer byteBuffer = state.stockTradeEncoder.buffer().byteBuffer();
        byte[] array = Arrays.copyOfRange(
                byteBuffer.array(),
                0,
                state.stockTradeEncoder.limit()
        );
        bh.consume(array);
    }

    @Benchmark
    public void measureSbeDeserializer(SbeState state, Blackhole bh) {
        state.decodeBuffer = ByteBuffer.wrap(state.bytes);
        state.decodeUnsafeBuffer.wrap(state.decodeBuffer);
        state.stockTradeDecoder.wrapAndApplyHeader(state.decodeUnsafeBuffer, 0, state.messageHeaderDecoder);
        bh.consume(state.stockTradeDecoder);
    }

}
