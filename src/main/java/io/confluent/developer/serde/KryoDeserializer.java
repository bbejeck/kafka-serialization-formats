package io.confluent.developer.serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import io.confluent.developer.Exchange;
import io.confluent.developer.Stock;
import io.confluent.developer.TxnType;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * User: Bill Bejeck
 * Date: 5/15/24
 * Time: 5:02 PM
 */
public class KryoDeserializer implements Deserializer<Stock> {

    private final Kryo kryo = new Kryo();
    private final Input input = new Input();

    public KryoDeserializer() {
        kryo.register(Stock.class);
        kryo.register(TxnType.class);
        kryo.register(Exchange.class);
    }

    @Override
    public Stock deserialize(String s, byte[] bytes) {
        input.setBuffer(bytes);
        return kryo.readObject(input, Stock.class);
    }
}
