package io.confluent.developer.jmh;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.developer.Stock;
import io.confluent.developer.TxnType;
import io.confluent.developer.proto.Exchange;
import io.confluent.developer.proto.StockProto;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

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
@Measurement(iterations = 15)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class SerializationBenchmarks {

    @State(Scope.Benchmark)
   public static class JacksonState {
       public ObjectMapper mapper;
       public io.confluent.developer.Stock jrSTock;

        public JacksonState() {
        }

        @Setup(Level.Trial)
       public void setup() {
           mapper = new ObjectMapper();
           jrSTock = new Stock(100.00, 10_000, "CFLT", "NASDAQ", TxnType.BUY);
        }
   }

   @State(Scope.Benchmark)
   public static class ProtoState {
       KafkaProtobufSerializer<StockProto> protobufSerializer = new KafkaProtobufSerializer<>();
       KafkaProtobufDeserializer<StockProto> protobufDeserializer = new KafkaProtobufDeserializer<>();
       byte[] serializedStock;
       StockProto stockProto;

       public ProtoState() {
       }

       @Setup(Level.Trial)
       public void setup() {
           StockProto.Builder stockBuilder = StockProto.newBuilder();
           stockBuilder.setSymbol("CFLT")
                   .setPrice(100.00)
                   .setShares(10_000)
                   .setExchange(Exchange.NASDAQ)
                   .setTxn(io.confluent.developer.proto.TxnType.BUY);
           Map<String,Object> config = new HashMap<>();
           config.put("schema.registry.url", "mock://localhost:8081");
           protobufSerializer.configure(config, false);
           stockProto = stockBuilder.build();
           serializedStock = protobufSerializer.serialize("dummy", stockProto);
       }
   }

    @Benchmark
    public void measureJacksonToByteArray(JacksonState state, Blackhole bh) throws JsonProcessingException {
        bh.consume(state.mapper.writeValueAsBytes(state.jrSTock));
    }

    @Benchmark
    public void measureProtobufToByteArray(ProtoState state, Blackhole bh) {
        byte[] protoBytes = state.protobufSerializer.serialize("topic", state.stockProto);
        bh.consume(protoBytes);
    }

    public static void main(String[] args) throws RunnerException {
            Options opt = new OptionsBuilder()
                    .include(SerializationBenchmarks.class.getName())
                    .forks(1)
                    .warmupIterations(5)
                    .measurementIterations(15)
                    .mode(Mode.Throughput)
                    .timeUnit(TimeUnit.MICROSECONDS)
                    .build();

            new Runner(opt).run();
    }


    
}
