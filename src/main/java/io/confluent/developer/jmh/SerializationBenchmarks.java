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
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
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
            config.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, StockProto.class);
            protobufDeserializer.configure(config, false);
            stockProto = stockBuilder.build();
            serializedStock = protobufSerializer.serialize("dummy", stockProto);
        }
    }


    @State(Scope.Benchmark)
    public static class SbeState {
        MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
        StockTradeEncoder stockTradeEncoder = new StockTradeEncoder();
        MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        StockTradeDecoder stockTradeDecoder = new StockTradeDecoder();
        byte[] bytes;
        Serializer<ByteBuffer> byteBufferSerializer = new ByteBufferSerializer();
        Deserializer<ByteBuffer> byteBufferDeserializer = new ByteBufferDeserializer();

        @Setup(Level.Trial)
        public void setUp() {
            ByteBuffer localDirectByteBuffer = ByteBuffer.allocateDirect(26);
            UnsafeBuffer localDirectUnsafeBuffer = new UnsafeBuffer(localDirectByteBuffer);
            stockTradeEncoder.wrapAndApplyHeader(localDirectUnsafeBuffer, 0, messageHeaderEncoder)
                    .price(100.00)
                    .shares(10_000)
                    .symbol("CFLT")
                    .exchange(baseline.Exchange.NASDAQ)
                    .txnType(baseline.TxnType.BUY);

            bytes = byteBufferSerializer.serialize("topic", stockTradeEncoder.buffer().byteBuffer());

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
    public void measureKafkaProtobufSerialization(ProtoState state, Blackhole bh) {
        byte[] protoBytes = state.protobufSerializer.serialize("topic", state.stockProto);
        bh.consume(protoBytes);
    }

    @Benchmark
    public void measureKafkaProtobufDeserialization(ProtoState state, Blackhole bh) {
        StockProto stockProto = state.protobufDeserializer.deserialize("topic", state.serializedStock);
        bh.consume(stockProto);
    }

    @Benchmark
    public void measureRawProtobufSerializationToByteArray(ProtoState state, Blackhole bh)  {
        byte[] protoBytes = state.stockProto.toByteArray();
        bh.consume(protoBytes);
    }

    @Benchmark
    public void measureSbeSerializationDirectByteBuffer(SbeState state, Blackhole bh) {
        StockTradeEncoder stockTradeEncoder = state.stockTradeEncoder;
        byte[] bytes = state.byteBufferSerializer.serialize("topic", stockTradeEncoder.buffer().byteBuffer());
        bh.consume(bytes);
    }

    @Benchmark
    public void measureSbeDeserializationDirectByteBuffer(SbeState state, Blackhole bh) {
        StockTradeDecoder stockTradeDecoder = state.stockTradeDecoder;
        byte[] bytes = state.bytes;
        ByteBuffer deserializedByteBuffer =  state.byteBufferDeserializer.deserialize("topic", bytes);
        UnsafeBuffer unsafeBuffer = new UnsafeBuffer(deserializedByteBuffer);
        stockTradeDecoder.wrapAndApplyHeader(unsafeBuffer, 0, state.messageHeaderDecoder);
        bh.consume(state.stockTradeDecoder);
    }

}
