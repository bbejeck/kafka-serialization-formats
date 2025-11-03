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
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.fory.Fory;
import org.apache.fory.config.CompatibleMode;
import org.capnproto.ArrayOutputStream;
import org.capnproto.MessageBuilder;
import org.capnproto.MessageReader;
import org.capnproto.Serialize;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * User: Bill Bejeck
 * Date: 5/3/24
 * Time: 8:45 AM
 */
class SerializationTests {

    private final SbeRecordSupplier sbeRecordSupplier = new SbeRecordSupplier();
    private final KafkaProtobufSerializer<StockProto> protobufSerializer = new KafkaProtobufSerializer<>();
    private final SbeDeserializer sbeNonDirectDeserializer = new SbeDeserializer();
    private final SbeSerializer sbeSerializer = new SbeSerializer();
    private final JacksonRecordSerializer jacksonRecordSerializer = new JacksonRecordSerializer();
    private final KryoSerializer kryoSerializer = new KryoSerializer();
    private final double price = 99.99;
    private final int shares = 3_000;

    @BeforeEach
    void setUp() {
        Map<String,Object> config = new HashMap<>();
        config.put("schema.registry.url", "mock://localhost:8081");
        protobufSerializer.configure(config, false);
    }

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
       StockTradeEncoder stockTradeEncoder = sbeRecordSupplier.get();
       byte[] protoSerialized = protobufSerializer.serialize("topic", stockProto);
       byte[] sbeBytes =  sbeSerializer.serialize("topic", stockTradeEncoder);
       byte[] kryoBytes =  kryoSerializer.serialize("topic", stock);
       byte[] jacksonBytes = jacksonRecordSerializer.serialize("topic", stock);

       assertEquals(27, protoSerialized.length);
       assertEquals(26, sbeBytes.length);
       assertEquals(26, kryoBytes.length);
       assertEquals(79, jacksonBytes.length);
    }

    @Test
    void sbeNonDirectEncodeDecodeTest() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
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
        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
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
    void shouldHandleObjectSchemaChanges() {
        Fory fory = Fory.builder()
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .build();
        fory.register(CustomerTrade.class);
        fory.register(CustomerTradeV2.class);

        CustomerTrade customerTrade = new CustomerTrade("Hulk", "hulk@avengers");
        CustomerTradeV2 customerTradeV2 = new CustomerTradeV2("Hulk", "hulk@avengers", "123 Stark Avenue, NYC");
        
        byte[] serialized = fory.serializeJavaObject(customerTrade);
        byte[] serializedV2 = fory.serializeJavaObject(customerTradeV2);

        CustomerTradeV2 deserializedV2 = fory.deserializeJavaObject(serialized, CustomerTradeV2.class);
        CustomerTrade deserializedV1 = fory.deserializeJavaObject(serializedV2, CustomerTrade.class);

        assertEquals(customerTrade.getName(), deserializedV2.getName());
        assertEquals(customerTrade.getEmail(), deserializedV2.getEmail());
        assertNull(deserializedV2.getAddress());

        assertEquals(customerTradeV2.getName(), deserializedV1.getName());
        assertEquals(customerTradeV2.getEmail(), deserializedV1.getEmail());

    }


    @Test
    void sbeSerializerDeserializerTest() {
        ByteBuffer directBuffer = ByteBuffer.allocateDirect(1024);
        StockTradeEncoder stockTradeEncoder = stockTradeEncoder(price, shares, directBuffer);
        byte[] sbeBytes =  sbeSerializer.serialize("topic", stockTradeEncoder);
        assertEquals(26, sbeBytes.length);
        StockTradeDecoder stockTradeDecoder = sbeNonDirectDeserializer.deserialize("topic", sbeBytes);
        assertEquals(price, stockTradeDecoder.price());
        assertEquals(shares, stockTradeDecoder.shares());
        assertEquals("CFLT", stockTradeDecoder.symbol());
        assertEquals(Exchange.NASDAQ, stockTradeDecoder.exchange());
        assertEquals(TxnType.BUY, stockTradeDecoder.txnType());
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

}
