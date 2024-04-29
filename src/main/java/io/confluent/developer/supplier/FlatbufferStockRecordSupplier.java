package io.confluent.developer.supplier;

import com.google.flatbuffers.FlatBufferBuilder;
import io.confluent.developer.TxnType;
import io.confluent.developer.flatbuffer.Stock;
import net.datafaker.Faker;

import java.util.function.Supplier;

/**
 * Supplier to provide a new Stock instance with each call to get() with randomized fields
 * This class IS NOT THREAD SAFE
 */
public class FlatbufferStockRecordSupplier implements Supplier<Stock> {

    private final Faker faker;
    private final FlatBufferBuilder builder = new FlatBufferBuilder();

    FlatbufferStockRecordSupplier(Faker faker) {
        this.faker = faker;
    }

    public FlatbufferStockRecordSupplier() {
        this(new Faker());
    }

    @Override
    public Stock get() {
        builder.clear();
        int symbol = builder.createString(faker.stock().nsdqSymbol());
        int exchange = builder.createString(faker.stock().exchanges());
        int fullName = builder.createString(faker.company().name());
        Stock.startStock(builder);
        Stock.addSymbol(builder, symbol);
        Stock.addExchange(builder,exchange);
        Stock.addFullName(builder, fullName);
        Stock.addType(builder, (byte) TxnType.values()[faker.number().numberBetween(0,2)].ordinal());
        Stock.addPrice(builder, faker.number().randomDouble(2,1,200));
        Stock.addShares(builder, faker.number().numberBetween(100, 10_000));
        int finishedStock = Stock.endStock(builder);
        builder.finish(finishedStock);
        return Stock.getRootAsStock(builder.dataBuffer());

    }
}
