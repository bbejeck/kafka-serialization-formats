package io.confluent.developer.supplier;

import io.confluent.developer.avro.Exchange;
import io.confluent.developer.avro.StockAvro;
import io.confluent.developer.avro.TxnType;
import net.datafaker.Faker;

import java.util.function.Supplier;

/**
 * User: Bill Bejeck
 * Date: 4/30/24
 * Time: 12:09 PM
 */
public class AvroStockSupplier implements Supplier<StockAvro> {

    private final Faker faker = new Faker();
    private final StockAvro.Builder builder = StockAvro.newBuilder();
    @Override
    public StockAvro get() {
        return builder.setPrice(faker.number().randomDouble(2, 1, 200))
                .setShares(faker.number().numberBetween(100, 10_000))
                .setSymbol(faker.stock().nsdqSymbol())
                .setExchange(Exchange.values()[faker.number().numberBetween(0,2)])
                .setType(TxnType.values()[faker.number().numberBetween(0, 2)]).build();
    }
}
