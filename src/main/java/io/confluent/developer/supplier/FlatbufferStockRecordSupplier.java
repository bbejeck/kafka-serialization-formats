package io.confluent.developer.supplier;

import com.google.flatbuffers.FlatBufferBuilder;
import io.confluent.developer.TxnType;
import io.confluent.developer.flatbuffer.StockFlatbuffer;
import net.datafaker.Faker;

import java.util.function.Supplier;

/**
 * Supplier to provide a new StockFlatbuffer instance with each call to get() with randomized fields
 * This class IS NOT THREAD SAFE
 */
public class FlatbufferStockRecordSupplier implements Supplier<StockFlatbuffer> {

    private final Faker faker;
    private final FlatBufferBuilder builder = new FlatBufferBuilder();

    FlatbufferStockRecordSupplier(Faker faker) {
        this.faker = faker;
    }

    public FlatbufferStockRecordSupplier() {
        this(new Faker());
    }

    @Override
    public StockFlatbuffer get() {
        builder.clear();
        int symbol = builder.createString(faker.stock().nsdqSymbol());
        int exchange = builder.createString(faker.stock().exchanges());
        StockFlatbuffer.startStockFlatbuffer(builder);
        StockFlatbuffer.addSymbol(builder, symbol);
        StockFlatbuffer.addExchange(builder,exchange);
        StockFlatbuffer.addType(builder, (byte) TxnType.values()[faker.number().numberBetween(0,2)].ordinal());
        StockFlatbuffer.addPrice(builder, faker.number().randomDouble(2,1,200));
        StockFlatbuffer.addShares(builder, faker.number().numberBetween(100, 10_000));
        int finishedStock = StockFlatbuffer.endStockFlatbuffer(builder);
        builder.finish(finishedStock);
        return StockFlatbuffer.getRootAsStockFlatbuffer(builder.dataBuffer());

    }
}
