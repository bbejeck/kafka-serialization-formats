package io.confluent.developer;

/**
 * Bill Bejeck
 * 4/25/24
 */

public record Stock(double price,
                    long shares,
                    String symbol,
                    Exchange exchange,
                    TxnType type) {
}
