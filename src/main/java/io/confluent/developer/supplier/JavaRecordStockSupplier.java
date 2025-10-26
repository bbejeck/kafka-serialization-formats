package io.confluent.developer.supplier;

import io.confluent.developer.Exchange;
import io.confluent.developer.Stock;
import io.confluent.developer.TxnType;
import net.datafaker.Faker;

import java.util.function.Supplier;

/**
 * User: Bill Bejeck
 * Date: 4/29/24
 * Time: 12:25 PM
 */
public class JavaRecordStockSupplier implements Supplier<Stock> {

    private final Faker faker = new Faker();

    @Override
    public Stock get() {
        return new Stock(faker.number().randomDouble(2, 1, 200),
                        faker.number().numberBetween(100, 10_000),
                        faker.stock().nsdqSymbol(),
                        Exchange.values()[faker.number().numberBetween(0, 2)],
                        TxnType.values()[faker.number().numberBetween(0, 2)]);
    }
}
