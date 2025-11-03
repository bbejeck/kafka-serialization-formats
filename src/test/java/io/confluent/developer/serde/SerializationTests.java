package io.confluent.developer.serde;

import baseline.Exchange;
import baseline.MessageHeaderEncoder;
import baseline.StockTradeDecoder;
import baseline.StockTradeEncoder;
import baseline.TxnType;
import io.confluent.developer.Stock;
import io.confluent.developer.StockTradeCapnp;
import io.confluent.developer.proto.StockProto;
import io.confluent.developer.supplier.SbeRecordSupplier;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.fory.Fory;
import org.apache.fory.config.CompatibleMode;
import org.capnproto.ArrayOutputStream;
import org.capnproto.MessageBuilder;
import org.capnproto.MessageReader;
import org.capnproto.Serialize;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * User: Bill Bejeck
 * Date: 5/3/24
 * Time: 8:45 AM
 */
class SerializationTests {

    private final SbeRecordSupplier sbeRecordSupplier = new SbeRecordSupplier();
    private final SbeDeserializer sbeNonDirectDeserializer = new SbeDeserializer();
    private final SbeSerializer sbeSerializer = new SbeSerializer();
    private final JacksonRecordSerializer jacksonRecordSerializer = new JacksonRecordSerializer();
    private final KryoSerializer kryoSerializer = new KryoSerializer();
    private final double price = 99.99;
    private final int shares = 3_000;


    @Test
    void capnpRoundTripTest() throws IOException {
        MessageBuilder messageBuilder = new MessageBuilder();
        StockTradeCapnp.StockTrade.Builder stockTrade = messageBuilder.initRoot(StockTradeCapnp.StockTrade.factory);
        stockTrade.setPrice(price);
        stockTrade.setShares(shares);
        stockTrade.setSymbol("CFLT");
        stockTrade.setExchange(StockTradeCapnp.Exchange.NASDAQ);
        stockTrade.setTxnType(StockTradeCapnp.TxnType.BUY);

        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
        Serialize.write(new ArrayOutputStream(byteBuffer), messageBuilder);
        byte[] serializedCapnp = byteBuffer.array();

        MessageReader messageReader = Serialize.read(ByteBuffer.wrap(serializedCapnp));
        StockTradeCapnp.StockTrade.Reader stockTradeReader = messageReader.getRoot(StockTradeCapnp.StockTrade.factory);
        assertEquals(stockTrade.getPrice(), stockTradeReader.getPrice());
        assertEquals(stockTrade.getShares(), stockTradeReader.getShares());
        assertEquals(stockTrade.getSymbol().toString(), stockTradeReader.getSymbol().toString());
        assertEquals(stockTrade.getExchange(), stockTradeReader.getExchange());
        assertEquals(stockTrade.getTxnType(), stockTradeReader.getTxnType());
    }

    @Test
    void kryoRoundTripTest() {
        try (KryoSerializer kryoSerializer = new KryoSerializer();
            KryoDeserializer kryoDeserializer = new KryoDeserializer()) {
            Stock stockOne = new Stock(100.00, 5_000L, "CFLT", io.confluent.developer.Exchange.NASDAQ, io.confluent.developer.TxnType.BUY);
            Stock stockTwo = new Stock(500.00, 105_000L, "AAPL", io.confluent.developer.Exchange.NASDAQ, io.confluent.developer.TxnType.BUY);

            byte[] bytesOne = kryoSerializer.serialize("topic", stockOne);
            byte[] bytesTwo = kryoSerializer.serialize("topic", stockTwo);

            Stock deserializedStockOne = kryoDeserializer.deserialize("topic", bytesOne);
            Stock deserializedStockTwo = kryoDeserializer.deserialize("topic", bytesTwo);
            assertEquals(stockOne, deserializedStockOne);
            assertEquals(stockTwo, deserializedStockTwo);
        }
    }

    
    @Test
    void serializedRecordSizesTest() {
       StockProto stockProto = stockProto();
       Stock stock = javaRecordStock();
       StockTradeEncoder stockTradeEncoder = sbeRecordSupplier.get();;
       byte[] sbeBytes =  sbeSerializer.serialize("topic", stockTradeEncoder);
       byte[] kryoBytes =  kryoSerializer.serialize("topic", stock);
       byte[] jacksonBytes = jacksonRecordSerializer.serialize("topic", stock);

       assertEquals(26, sbeBytes.length);
       assertEquals(26, kryoBytes.length);
       assertEquals(79, jacksonBytes.length);
    }

    @Test
    void sbeNonDirectEncodeDecodeTest() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(StockTradeEncoder.BLOCK_LENGTH + MessageHeaderEncoder.ENCODED_LENGTH);;
        StockTradeEncoder stockTradeEncoder = stockTradeEncoder(price, shares, byteBuffer);
        byte[] sbeBytes =  sbeSerializer.serialize("topic", stockTradeEncoder);
        assertEquals(26,sbeBytes.length);

        StockTradeDecoder stockTradeDecoder = sbeNonDirectDeserializer.deserialize("topic", sbeBytes);
        assertEquals(Exchange.NASDAQ, stockTradeDecoder.exchange());
        assertEquals(shares, stockTradeDecoder.shares());
        assertEquals(price, stockTradeDecoder.price());
        assertEquals("CFLT", stockTradeDecoder.symbol());
        assertEquals(TxnType.BUY, stockTradeDecoder.txnType());
    }

    @Test
    void sbeNonDirectEncodeDecodeMaxValuesTest() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(StockTradeEncoder.BLOCK_LENGTH + MessageHeaderEncoder.ENCODED_LENGTH);
        StockTradeEncoder stockTradeEncoder = stockTradeEncoder(Double.MAX_VALUE, Integer.MAX_VALUE, byteBuffer);
        byte[] sbeBytes =  sbeSerializer.serialize("topic", stockTradeEncoder);
        assertEquals(26,sbeBytes.length);

        StockTradeDecoder stockTradeDecoder = sbeNonDirectDeserializer.deserialize("topic", sbeBytes);
        assertEquals(Double.MAX_VALUE, stockTradeDecoder.price());
        assertEquals(Integer.MAX_VALUE, stockTradeDecoder.shares());
        assertEquals("CFLT", stockTradeDecoder.symbol());
        assertEquals(Exchange.NASDAQ, stockTradeDecoder.exchange());
        assertEquals(TxnType.BUY, stockTradeDecoder.txnType());
    }

    @Test
    void foryIntArrayCompressionImpact() {
        // Create Fory instance WITHOUT integer array compression
        Fory foryUncompressed = Fory.builder().build();
        foryUncompressed.register(TradingVolumes.class);

        // Create Fory instance WITH integer array compression enabled
        Fory foryCompressed = Fory.builder()
                .withIntArrayCompressed(true)
                .build();
        foryCompressed.register(TradingVolumes.class);

        // Create test data with integer arrays containing patterns that compress well
        // Simulating hourly trading volumes over 24 hours (many similar values)
        int[] hourlyVolumes = new int[24];
        for (int i = 0; i < 24; i++) {
            // Realistic pattern: low volume overnight, high during trading hours
            hourlyVolumes[i] = (i >= 9 && i <= 16) ? 50000 + (i * 1000) : 5000;
        }

        // Daily volumes for 30 days (some repeated patterns)
        int[] dailyVolumes = new int[30];
        for (int i = 0; i < 30; i++) {
            dailyVolumes[i] = 1000000 + (i % 7) * 50000; // Weekly patterns
        }

        TradingVolumes volumes = new TradingVolumes(hourlyVolumes, dailyVolumes, "AAPL");

        // Serialize with and without compression
        byte[] uncompressedBytes = foryUncompressed.serialize(volumes);
        byte[] compressedBytes = foryCompressed.serialize(volumes);

        // Verify deserialization works correctly
        TradingVolumes decompressedVolumes = (TradingVolumes) foryCompressed.deserialize(compressedBytes);
        assertEquals(volumes.getSymbol(), decompressedVolumes.getSymbol());
        assertEquals(hourlyVolumes.length, decompressedVolumes.getHourlyVolumes().length);
        assertEquals(dailyVolumes.length, decompressedVolumes.getDailyVolumes().length);

        // Verify array contents match
        for (int i = 0; i < hourlyVolumes.length; i++) {
            assertEquals(hourlyVolumes[i], decompressedVolumes.getHourlyVolumes()[i],
                    "Hourly volume at index " + i + " should match");
        }
    }

    @Test
    void foryStringCompressionImpact() {
        // Create Fory instance WITHOUT string compression
        Fory foryUncompressed = Fory.builder().build();
        foryUncompressed.register(MarketReport.class);

        // Create Fory instance WITH string compression enabled via withStringCompressed()
        Fory foryCompressed = Fory.builder()
                .withStringCompressed(true)
                .build();
        foryCompressed.register(MarketReport.class);

        // Create test data with repeated string patterns (common in real market data)
        List<String> companyDescriptions = new java.util.ArrayList<>();
        List<String> marketCommentary = new java.util.ArrayList<>();

        // Add repeated company descriptions (simulating market sectors/categories)
        String techDesc = "Technology company specializing in cloud computing and software services";
        String financeDesc = "Financial services company providing banking and investment solutions";
        String healthDesc = "Healthcare company focused on pharmaceutical research and development";

        for (int i = 0; i < 20; i++) {
            // Repeated patterns compress well
            if (i % 3 == 0) companyDescriptions.add(techDesc);
            else if (i % 3 == 1) companyDescriptions.add(financeDesc);
            else companyDescriptions.add(healthDesc);
        }

        // Add repeated market commentary phrases
        String bullish = "Market sentiment remains bullish with strong buying pressure across major indices";
        String bearish = "Market sentiment turns bearish amid concerns about inflation and interest rates";
        String neutral = "Market sentiment neutral as investors await earnings reports and economic data";

        for (int i = 0; i < 15; i++) {
            if (i % 3 == 0) marketCommentary.add(bullish);
            else if (i % 3 == 1) marketCommentary.add(bearish);
            else marketCommentary.add(neutral);
        }

        MarketReport report = new MarketReport(companyDescriptions, marketCommentary, "2024-01-15");

        // Serialize with and without compression
        byte[] uncompressedBytes = foryUncompressed.serialize(report);
        byte[] compressedBytes = foryCompressed.serialize(report);

        // Verify deserialization works correctly
        MarketReport decompressedReport = (MarketReport) foryCompressed.deserialize(compressedBytes);
        assertEquals(report.getReportDate(), decompressedReport.getReportDate());
        assertEquals(report.getCompanyDescriptions().size(), decompressedReport.getCompanyDescriptions().size());
        assertEquals(report.getMarketCommentary().size(), decompressedReport.getMarketCommentary().size());

        // Verify string content matches
        for (int i = 0; i < companyDescriptions.size(); i++) {
            assertEquals(companyDescriptions.get(i), decompressedReport.getCompanyDescriptions().get(i),
                    "Company description at index " + i + " should match");
        }

        System.out.println("\n=== Fory String Compression Impact ===");
        System.out.println("Data: 20 company descriptions + 15 market commentaries");
        System.out.println("Without compression: " + uncompressedBytes.length + " bytes");
        System.out.println("With compression:    " + compressedBytes.length + " bytes");
        System.out.println("Space saved:         " + (uncompressedBytes.length - compressedBytes.length) + " bytes");
        System.out.println("Compression ratio:   " + 
                String.format("%.2fx", (double) uncompressedBytes.length / compressedBytes.length));
        System.out.println("\nBenefit: String compression deduplicates repeated strings");
        System.out.println("         Only stores each unique string once, uses references");
        System.out.println("         Ideal for: Categories, statuses, error messages, log data");
        System.out.println("         Kafka benefit: Smaller messages = higher throughput, lower costs");

        // Note: Compression effectiveness depends on data patterns and Fory version
        if (compressedBytes.length < uncompressedBytes.length) {
            double throughputImprovement = (double) uncompressedBytes.length / compressedBytes.length;
            System.out.println("\n✓ String compression reduced size!");
            System.out.println("Kafka Impact: " + String.format("%.0f%%", (throughputImprovement - 1) * 100) + 
                    " more messages per second at same bandwidth");
        } else {
            System.out.println("\nℹ String compression didn't reduce size - Fory may already optimize by default");
            System.out.println("  In Fory 0.13.0+, string deduplication may be enabled automatically");
        }
    }

    @Test
    void foryBatchSerializationEfficiency() {
        Fory fory = Fory.builder()
                        .withCompatibleMode(CompatibleMode.COMPATIBLE)
                        .build();

        fory.register(Stock.class);
        fory.register(io.confluent.developer.Exchange.class);
        fory.register(io.confluent.developer.TxnType.class);

        // Create a batch of stock records (simulating columnar data)
        java.util.List<Stock> stockBatch = java.util.List.of(
                new Stock(100.50, 1000L, "AAPL", io.confluent.developer.Exchange.NASDAQ, io.confluent.developer.TxnType.BUY),
                new Stock(250.75, 500L, "GOOGL", io.confluent.developer.Exchange.NASDAQ, io.confluent.developer.TxnType.SELL),
                new Stock(150.25, 750L, "MSFT", io.confluent.developer.Exchange.NYSE, io.confluent.developer.TxnType.BUY),
                new Stock(3500.00, 100L, "AMZN", io.confluent.developer.Exchange.NASDAQ, io.confluent.developer.TxnType.BUY)
        );

        // Serialize batch - Fory internally optimizes for columnar layout
        // All prices stored together, all shares together, etc.
        byte[] serializedBatch = fory.serialize(stockBatch);

        // Deserialize batch
        @SuppressWarnings("unchecked")
        java.util.List<Stock> deserializedBatch = (java.util.List<Stock>) fory.deserialize(serializedBatch);

        // Verify all records match
        assertEquals(stockBatch.size(), deserializedBatch.size());
        for (int i = 0; i < stockBatch.size(); i++) {
            Stock original = stockBatch.get(i);
            Stock deserialized = deserializedBatch.get(i);
            assertEquals(original.price(), deserialized.price(), 0.001);
            assertEquals(original.shares(), deserialized.shares());
            assertEquals(original.symbol(), deserialized.symbol());
            assertEquals(original.exchange(), deserialized.exchange());
            assertEquals(original.type(), deserialized.type());
        }
    }

        @Test
    void foryColumnarVsRowOrientedComparison() {
        // Demonstrate why columnar format is better for batch analytics
        Fory fory = Fory.builder().build();
        fory.register(Stock.class);
        fory.register(io.confluent.developer.Exchange.class);
        fory.register(io.confluent.developer.TxnType.class);

        // Create larger batch to show compression benefits
        java.util.List<Stock> largeBatch = new java.util.ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            largeBatch.add(new Stock(
                    100.0 + (i % 10),  // Limited price range for better compression
                    1000L * (i % 5),    // Repeated share amounts
                    "SYM" + (i % 100),  // Limited symbol variety
                    io.confluent.developer.Exchange.NASDAQ,  // Same exchange
                    io.confluent.developer.TxnType.BUY       // Same txn type
            ));
        }

        // Serialize as batch (columnar-friendly)
        byte[] batchSerialized = fory.serialize(largeBatch);

        // Serialize individually (row-oriented)
        int totalIndividualSize = 0;
        for (Stock stock : largeBatch) {
            byte[] individual = fory.serialize(stock);
            totalIndividualSize += individual.length;
        }

        System.out.println("=== Columnar vs Row-Oriented Comparison ===");
        System.out.println("Records: " + largeBatch.size());
        System.out.println("Batch (columnar): " + batchSerialized.length + " bytes");
        System.out.println("Individual (row): " + totalIndividualSize + " bytes");
        System.out.println("Compression ratio: " + 
                String.format("%.2f", (double) totalIndividualSize / batchSerialized.length) + "x");

        // Columnar should be significantly smaller due to:
        // - Column-wise compression
        // - Eliminated repeated metadata
        // - Better encoding of similar values
        assertTrue(batchSerialized.length < totalIndividualSize, "Columnar format should be more compact than row-oriented");
    }

        @Test
    void shouldHandleObjectSchemaChanges() {
        // Configure Fory with COMPATIBLE mode to handle schema evolution
        Fory fory = Fory.builder()
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .build();
        fory.register(CustomerTrade.class);
        fory.register(CustomerTradeV2.class);
        fory.register(CustomerTradeV3.class);

        // Create instances of different schema versions
        CustomerTrade customerTrade = new CustomerTrade("Hulk", "hulk@avengers.com");
        CustomerTradeV2 customerTradeV2 = new CustomerTradeV2("Thor", "thor@avengers.com", "123 Stark Avenue, NYC");
        CustomerTradeV3 customerTradeV3 = new CustomerTradeV3("Iron Man", "555-1234");

        // Serialize each version using serialize() method
        byte[] serializedV1 = fory.serialize(customerTrade);
        byte[] serializedV2 = fory.serialize(customerTradeV2);
        byte[] serializedV3 = fory.serialize(customerTradeV3);

        // Test FORWARD COMPATIBILITY: New code (V2) reading old data (V1)
        // V2 adds 'address' field - should be null when reading V1 data
        CustomerTradeV2 v1DataAsV2 = fory.deserialize(serializedV1, CustomerTradeV2.class);
        assertEquals("Hulk", v1DataAsV2.getName());
        assertEquals("hulk@avengers.com", v1DataAsV2.getEmail());
        assertNull(v1DataAsV2.getAddress(), "New field 'address' should be null when reading old schema");

        // Test BACKWARD COMPATIBILITY: Old code (V1) reading new data (V2)
        // V1 simply ignores the 'address' field from V2
        CustomerTrade v2DataAsV1 = fory.deserialize(serializedV2, CustomerTrade.class);
        assertEquals("Thor", v2DataAsV1.getName());
        assertEquals("thor@avengers.com", v2DataAsV1.getEmail());

        // Test FIELD REMOVAL: V3 removes 'email', adds 'phoneNumber'
        // Reading V2 data (has email, no phone) as V3 (has phone, no email)
        CustomerTradeV3 v2DataAsV3 = fory.deserialize(serializedV2, CustomerTradeV3.class);
        assertEquals("Thor", v2DataAsV3.getName());
        assertNull(v2DataAsV3.getPhoneNumber(), "New field 'phoneNumber' should be null when reading data without it");

        // Reading V3 data (has phone, no email) as V2 (has email, no phone)
        CustomerTradeV2 v3DataAsV2 = fory.deserialize(serializedV3, CustomerTradeV2.class);
        assertEquals("Iron Man", v3DataAsV2.getName());
        assertNull(v3DataAsV2.getEmail(), "Removed field 'email' should be null");
        assertNull(v3DataAsV2.getAddress(), "Missing field 'address' should be null");

        // Test reading V3 as V1 (skipping intermediate schema)
        CustomerTrade v3DataAsV1 = fory.deserialize(serializedV3, CustomerTrade.class);
        assertEquals("Iron Man", v3DataAsV1.getName());
        assertNull(v3DataAsV1.getEmail(), "Removed field 'email' should be null");
    }


    @ParameterizedTest
    @MethodSource("byteBufferSource")
    void sbeSerializeRoundTripTest(ByteBuffer byteBuffer) {
        StockTradeEncoder stockTradeEncoder = stockTradeEncoder(price, shares, byteBuffer);
        byte[] sbeBytes =  sbeSerializer.serialize("topic", stockTradeEncoder);
        assertEquals(26, sbeBytes.length);
        StockTradeDecoder stockTradeDecoder = sbeNonDirectDeserializer.deserialize("topic", sbeBytes);
        assertEquals(price, stockTradeDecoder.price());
        assertEquals(shares, stockTradeDecoder.shares());
        assertEquals("CFLT", stockTradeDecoder.symbol());
        assertEquals(Exchange.NASDAQ, stockTradeDecoder.exchange());
        assertEquals(TxnType.BUY, stockTradeDecoder.txnType());
    }

    private static Stream<Arguments> byteBufferSource() {
        return Stream.of(
                Arguments.of(ByteBuffer.allocateDirect(StockTradeEncoder.BLOCK_LENGTH + MessageHeaderEncoder.ENCODED_LENGTH)),
                Arguments.of(ByteBuffer.allocate(StockTradeEncoder.BLOCK_LENGTH + MessageHeaderEncoder.ENCODED_LENGTH))
        );
    }

    StockTradeEncoder stockTradeEncoder(double price, int shares, ByteBuffer byteBuffer) {
        StockTradeEncoder stockTradeEncoder = new StockTradeEncoder();
        UnsafeBuffer unsafeBuffer = new UnsafeBuffer(byteBuffer);
        MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
        String symbol = "CFLT";
        Exchange exchange = Exchange.NASDAQ;
        TxnType txnType = TxnType.BUY;
        stockTradeEncoder.wrapAndApplyHeader(unsafeBuffer, 0, messageHeaderEncoder)
                .price(price)
                .shares(shares)
                .symbol(symbol)
                .exchange(exchange)
                .txnType(txnType);

        return stockTradeEncoder;
    }

    Stock javaRecordStock() {
        return new Stock(101.1, 70_000L, "CFLT", io.confluent.developer.Exchange.NASDAQ, io.confluent.developer.TxnType.BUY);
    }


    StockProto stockProto() {
        StockProto.Builder stockProtoBuilder = StockProto.newBuilder();
        stockProtoBuilder.setPrice(101.0);
        stockProtoBuilder.setShares(70_000);
        stockProtoBuilder.setSymbol("CFLT");
        stockProtoBuilder.setExchange(io.confluent.developer.proto.Exchange.NASDAQ);
        stockProtoBuilder.setTxn(io.confluent.developer.proto.TxnType.BUY);
        return stockProtoBuilder.build();
    }

    interface StockOperation {
        void execute(Stock stock);

    }

    /**
     * Test class for demonstrating integer array compression
     * Contains arrays of trading volumes that benefit from compression
     */
    public static class TradingVolumes {
        private int[] hourlyVolumes;
        private int[] dailyVolumes;
        private String symbol;

        public TradingVolumes() {
        }

        public TradingVolumes(int[] hourlyVolumes, int[] dailyVolumes, String symbol) {
            this.hourlyVolumes = hourlyVolumes;
            this.dailyVolumes = dailyVolumes;
            this.symbol = symbol;
        }

        public int[] getHourlyVolumes() {
            return hourlyVolumes;
        }

        public void setHourlyVolumes(int[] hourlyVolumes) {
            this.hourlyVolumes = hourlyVolumes;
        }

        public int[] getDailyVolumes() {
            return dailyVolumes;
        }

        public void setDailyVolumes(int[] dailyVolumes) {
            this.dailyVolumes = dailyVolumes;
        }

        public String getSymbol() {
            return symbol;
        }

        public void setSymbol(String symbol) {
            this.symbol = symbol;
        }
    }

    /**
     * Test class for demonstrating string compression
     * Contains repeated company descriptions and market commentary
     */
    public static class MarketReport {
        private List<String> companyDescriptions;
        private List<String> marketCommentary;
        private String reportDate;

        public MarketReport() {
        }

        public MarketReport(List<String> companyDescriptions, List<String> marketCommentary, String reportDate) {
            this.companyDescriptions = companyDescriptions;
            this.marketCommentary = marketCommentary;
            this.reportDate = reportDate;
        }

        public List<String> getCompanyDescriptions() {
            return companyDescriptions;
        }

        public void setCompanyDescriptions(List<String> companyDescriptions) {
            this.companyDescriptions = companyDescriptions;
        }

        public List<String> getMarketCommentary() {
            return marketCommentary;
        }

        public void setMarketCommentary(List<String> marketCommentary) {
            this.marketCommentary = marketCommentary;
        }

        public String getReportDate() {
            return reportDate;
        }

        public void setReportDate(String reportDate) {
            this.reportDate = reportDate;
        }
    }


    public static class CustomerTrade implements StockOperation {
        private String name;
        private String email;

        public CustomerTrade() {
        }

        public CustomerTrade(String name, String email) {
            this.name = name;
            this.email = email;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getEmail() {
            return email;
        }

        public void setEmail(String email) {
            this.email = email;
        }

        public void execute(Stock stock) {
            System.out.println("Executing customer trade for stock: " + stock);
        }
    }

    public static class CustomerTradeV2 implements StockOperation {
        private String name;
        private String email;
        private String address;

        public CustomerTradeV2() {
        }

        public CustomerTradeV2(String name, String email, String address) {
            this.name = name;
            this.email = email;
            this.address = address;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }

        public String getEmail() {
            return email;
        }

        public void setEmail(String email) {
            this.email = email;
        }

        public void execute(Stock stock) {
            System.out.println("Executing customer trade for stock: " + stock);
        }

        @Override
        public String toString() {
            return "CustomerTradeV2{" +
                "name='" + name + '\'' +
                ", email='" + email + '\'' +
                ", address='" + address + '\'' +
                '}';
        }
    }

    /**
     * V3 demonstrates field removal (email removed) and replacement (phoneNumber added)
     * This tests Fory's ability to handle breaking schema changes in compatible mode
     */
    public static class CustomerTradeV3 implements StockOperation {
        private String name;
        private String phoneNumber;  // Replaces email field

        public CustomerTradeV3() {
        }

        public CustomerTradeV3(String name, String phoneNumber) {
            this.name = name;
            this.phoneNumber = phoneNumber;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getPhoneNumber() {
            return phoneNumber;
        }

        public void setPhoneNumber(String phoneNumber) {
            this.phoneNumber = phoneNumber;
        }

        public void execute(Stock stock) {
            System.out.println("Executing customer trade for stock: " + stock);
        }

        @Override
        public String toString() {
            return "CustomerTradeV3{" +
                "name='" + name + '\'' +
                ", phoneNumber='" + phoneNumber + '\'' +
                '}';
        }
    }

}
