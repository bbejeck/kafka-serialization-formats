package io.confluent.developer.serde;

import io.confluent.developer.Stock;
import org.apache.fory.Fory;
import org.apache.kafka.common.serialization.Serializer;


public class ForySerializer implements Serializer<Stock> {

    private final Fory fory;

    public ForySerializer() {
        fory = Fory.builder().build();
        fory.register(Stock.class);
        fory.register(io.confluent.developer.Exchange.class);
        fory.register(io.confluent.developer.TxnType.class);
    }

    @Override
    public byte[] serialize(String s, Stock stock) {
        return fory.serialize(stock);
    }
}
