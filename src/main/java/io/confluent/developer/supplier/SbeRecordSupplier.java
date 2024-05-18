package io.confluent.developer.supplier;

import baseline.*;
import net.datafaker.Faker;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.Supplier;

/**
 * User: Bill Bejeck
 * Date: 5/3/24
 * Time: 4:04 PM
 */
public class SbeRecordSupplier implements Supplier<StockTradeEncoder> {
    ByteBuffer byteBuffer = ByteBuffer.allocate(26);
    UnsafeBuffer unsafeBuffer = new UnsafeBuffer(byteBuffer);
    MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    StockTradeEncoder stockTradeEncoder = new StockTradeEncoder();
    private final Faker faker = new Faker();

    @Override
    public StockTradeEncoder get() {
        byteBuffer.clear();
        String symbol = faker.stock().nsdqSymbol();
        symbol = symbol.length() > 4 ? symbol.substring(0, 4) : symbol;
        stockTradeEncoder.wrapAndApplyHeader(unsafeBuffer, 0, messageHeaderEncoder)
                .price((float) faker.number().randomDouble(2, 1, 200))
                .shares(faker.number().numberBetween(100, 10_000))
                .symbol(symbol)
                .exchange(Exchange.values()[faker.number().numberBetween(0,2)])
                .txnType(TxnType.values()[faker.number().numberBetween(0,2)]);

        return stockTradeEncoder;
    }
}
