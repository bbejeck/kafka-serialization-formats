package io.confluent.developer.jmh;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

/**
 * User: Bill Bejeck
 * Date: 4/25/24
 * Time: 5:13 PM
 */

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class SerializationBenchmarks {

    public ObjectMapper mapper;
    public io.confluent.developer.Stock jrSTock;

    @Setup(Level.Invocation)
    public void setup() {
    }



    @Benchmark
    public void measureJacksonToByteArray(Blackhole bh) throws JsonProcessingException {
        bh.consume(mapper.writeValueAsBytes(jrSTock));
    }


    
}
