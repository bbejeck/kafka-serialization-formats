package io.confluent.developer.serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import io.confluent.developer.Exchange;
import io.confluent.developer.Stock;
import io.confluent.developer.TxnType;
import org.apache.kafka.common.serialization.Serializer;

/**
 * User: Bill Bejeck
 * Date: 5/15/24
 * Time: 10:17 AM
 */
public class KryoSerializer implements Serializer<Stock>{

    private final Kryo kryo = new Kryo();
    private final Output output = new Output();

    public KryoSerializer() {
        kryo.register(Stock.class);
        kryo.register(TxnType.class);
        kryo.register(Exchange.class);
    }

    @Override
    public byte[] serialize(String s, Stock stock) {
        output.setBuffer(new byte[26]);
        kryo.writeObject(output, stock);
        return output.getBuffer();
    }
}
