package io.confluent.developer.supplier;

import io.confluent.developer.avro.StockAvro;

import io.confluent.developer.avro.txn;
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
                .setExchange(faker.stock().exchanges())
                .setFullName(faker.company().name())
                .setTxnType(txn.values()[faker.number().numberBetween(0, 2)]).build();
    }
}
