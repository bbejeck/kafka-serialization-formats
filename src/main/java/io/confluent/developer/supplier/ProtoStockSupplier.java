package io.confluent.developer.supplier;

import io.confluent.developer.proto.TxnType;
import io.confluent.developer.proto.StockProto;
import net.datafaker.Faker;

import java.util.function.Supplier;

/**
 * User: Bill Bejeck
 * Date: 4/29/24
 * Time: 12:25 PM
 */
public class ProtoStockSupplier implements Supplier<StockProto> {

    private final Faker faker = new Faker();
    private final StockProto.Builder builder = StockProto.newBuilder();

    @Override
    public StockProto get() {
        return builder.setPrice(faker.number().randomDouble(2, 1, 200))
                .setShares(faker.number().numberBetween(100, 10_000))
                .setSymbol(faker.stock().nsdqSymbol())
                .setExchange(faker.stock().exchanges())
                .setTxn(TxnType.values()[faker.number().numberBetween(0, 2)]).build();
    }
}
