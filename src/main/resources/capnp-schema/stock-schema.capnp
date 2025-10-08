@0xdbb9ad1f14bf0b36;  # unique file ID

using Java = import "java.capnp";
$Java.package("io.confluent.developer");
$Java.outerClassname("StockTradeCapnp");

# Transaction Type enumeration
enum TxnType {
    buy @0;
    sell @1;
}

# Market Exchange enumeration
enum Exchange {
    nasdaq @0;
    nyse @1;
}

# Stock Trade message structure
struct StockTrade {
    price @0 :Float64;
    shares @1 :UInt32;
    symbol @2 :Text;      # 4-character stock symbol
    exchange @3 :Exchange;
    txnType @4 :TxnType;
}
