package io.confluent.developer.supplier;

import baseline.Exchange;
import baseline.MessageHeaderEncoder;
import baseline.StockTradeEncoder;
import baseline.TxnType;
import net.datafaker.Faker;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Supplier;

/**
 * User: Bill Bejeck
 * Date: 5/3/24
 * Time: 4:04 PM
 */
public class SbeRecordSupplier implements Supplier<StockTradeEncoder> {
    ByteBuffer byteBuffer = ByteBuffer.allocate(StockTradeEncoder.BLOCK_LENGTH + MessageHeaderEncoder.ENCODED_LENGTH);
    UnsafeBuffer unsafeBuffer = new UnsafeBuffer(byteBuffer);
    MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    StockTradeEncoder stockTradeEncoder = new StockTradeEncoder();
    private final Faker faker = new Faker();
    private final List<String> symbols = new java.util.ArrayList<>();

    public SbeRecordSupplier() {
        for (int i = 0; i < 100; i++) {
            String symbol = faker.stock().nsdqSymbol();
            symbol = symbol.length() > 4 ? symbol.substring(0, 4) : symbol;
            symbols.add(symbol);
        }
    }

    @Override
    public StockTradeEncoder get() {
        byteBuffer.clear();
        String symbol = symbols.get(faker.number().numberBetween(0, symbols.size() - 1));
        stockTradeEncoder.wrapAndApplyHeader(unsafeBuffer, 0, messageHeaderEncoder)
                .price(150.00)
                .shares(faker.number().numberBetween(100, 10_000))
                .symbol(symbol)
                .exchange(Exchange.values()[faker.number().numberBetween(0,2)])
                .txnType(TxnType.values()[faker.number().numberBetween(0,2)]);

        return stockTradeEncoder;
    }
}
