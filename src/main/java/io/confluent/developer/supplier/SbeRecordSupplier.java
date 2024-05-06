package io.confluent.developer.supplier;

import baseline.Exchange;
import baseline.MessageHeaderEncoder;
import baseline.StockTradeEncoder;
import baseline.TxnType;
import net.datafaker.Faker;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.function.Supplier;

/**
 * User: Bill Bejeck
 * Date: 5/3/24
 * Time: 4:04 PM
 */
public class SbeRecordSupplier implements Supplier<byte[]> {
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    UnsafeBuffer unsafeBuffer = new UnsafeBuffer(byteBuffer);
    StockTradeEncoder stockTradeEncoder = new StockTradeEncoder();
    MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final Faker faker = new Faker();

    @Override
    public byte[] get() {
        byteBuffer.clear();
        String symbol = faker.stock().nsdqSymbol();
        symbol = symbol.length() > 4 ? symbol.substring(0, 4) : symbol;
        stockTradeEncoder.wrapAndApplyHeader(unsafeBuffer, 0, messageHeaderEncoder)
                .price((float) faker.number().randomDouble(2, 1, 200))
                .shares(faker.number().numberBetween(100, 10_000))
                .symbol(symbol)
                .exchange(Exchange.values()[faker.number().numberBetween(0,2)])
                .txnType(TxnType.values()[faker.number().numberBetween(0,2)]);
        byte[] array = null;
        if(byteBuffer.hasArray()) {
            array  = java.util.Arrays.copyOfRange(
                    byteBuffer.array(),
                   0,
                    stockTradeEncoder.limit()
            );
        }
        return array;
    }
}
