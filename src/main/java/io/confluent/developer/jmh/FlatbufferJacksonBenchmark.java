package io.confluent.developer.jmh;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.flatbuffers.FlatBufferBuilder;
import io.confluent.developer.TxnType;
import io.confluent.developer.flatbuffer.StockFlatbuffer;
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
public class FlatbufferJacksonBenchmark {

    public ObjectMapper mapper;
    public StockFlatbuffer fbStock;
    FlatBufferBuilder flatBufferBuilder;
    public io.confluent.developer.Stock jrSTock;

    @Setup(Level.Invocation)
    public void setup() {
        flatBufferBuilder = new FlatBufferBuilder();
        mapper = new ObjectMapper();
        int symbolName = flatBufferBuilder.createString("CFLT");
        int exchangeName = flatBufferBuilder.createString("NASDAQ");
        int type = TxnType.BUY.ordinal();
        int stock = StockFlatbuffer.createStockFlatbuffer(flatBufferBuilder, 100.0, 1000L, symbolName, exchangeName, (byte) type);
        flatBufferBuilder.finish(stock);
        fbStock = StockFlatbuffer.getRootAsStockFlatbuffer(flatBufferBuilder.dataBuffer());

        jrSTock = new io.confluent.developer.Stock(100.0, 1000L, "CFLT", "NASDAQ", TxnType.BUY);
    }

    @Benchmark
    public void measureFlatBufferToByteArray(Blackhole bh) {

        bh.consume(fbStock.exchangeAsByteBuffer().array());
    }

    @Benchmark
    public void measureJacksonToByteArray(Blackhole bh) throws JsonProcessingException {
        bh.consume(mapper.writeValueAsBytes(jrSTock));
    }


    
}
