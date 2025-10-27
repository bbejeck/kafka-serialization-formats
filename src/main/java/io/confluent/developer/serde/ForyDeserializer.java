package io.confluent.developer.serde;

import io.confluent.developer.Stock;
import org.apache.fory.Fory;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * User: Bill Bejeck
 * Date: 10/27/25
 * Time: 4:18 PM
 */
public class ForyDeserializer implements Deserializer<Stock> {
    private final Fory fory;

    public ForyDeserializer() {
        fory = Fory.builder().build();
        fory.register(Stock.class);
        fory.register(io.confluent.developer.Exchange.class);
        fory.register(io.confluent.developer.TxnType.class);
    }

    @Override
    public Stock deserialize(String s, byte[] bytes) {
        return fory.deserialize(bytes, Stock.class);
    }
}
