package io.confluent.developer.serde;

import baseline.*;
import com.esotericsoftware.kryo.Kryo;
import io.confluent.developer.Stock;
import io.confluent.developer.StockTradeCapnp;
import io.confluent.developer.avro.StockAvro;
import io.confluent.developer.proto.StockProto;
import io.confluent.developer.supplier.SbeRecordSupplier;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.agrona.concurrent.UnsafeBuffer;
import org.capnproto.MessageBuilder;
import org.capnproto.MessageReader;
import org.capnproto.Serialize;
import org.capnproto.SerializePacked;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * User: Bill Bejeck
 * Date: 5/3/24
 * Time: 8:45 AM
 */
class SerializationTests {

    private final SbeRecordSupplier sbeRecordSupplier = new SbeRecordSupplier();
    private final KafkaProtobufSerializer<StockProto> protobufSerializer = new KafkaProtobufSerializer<>();
    private final KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer();
    private final SbeDeserializer sbeNonDirectDeserializer = new SbeDeserializer();
    private final SbeSerializer sbeSerializer = new SbeSerializer();
    private final JacksonRecordSerializer jacksonRecordSerializer = new JacksonRecordSerializer();
    private final KryoSerializer kryoSerializer = new KryoSerializer();
    private final MessageBuilder messageBuilder = new MessageBuilder();
    private final double price = 99.99;
    private final int shares = 3_000;

    @BeforeEach
    void setUp() {
        Map<String,Object> config = new HashMap<>();
        config.put("schema.registry.url", "mock://localhost:8081");
        protobufSerializer.configure(config, false);
        avroSerializer.configure(config, false);
    }

    @Test
    void capnpRoundTripTest() throws IOException {
        StockTradeCapnp.StockTrade.Builder stockTrade = messageBuilder.initRoot(StockTradeCapnp.StockTrade.factory);
        stockTrade.setPrice(price);
        stockTrade.setShares(shares);
        stockTrade.setSymbol("CFLT");
        stockTrade.setExchange(StockTradeCapnp.Exchange.NASDAQ);
        stockTrade.setTxnType(StockTradeCapnp.TxnType.BUY);
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ByteBuffer[] segments = messageBuilder.getSegmentsForOutput();
        for (ByteBuffer segment : segments) {
            byte[] segmentBytes = new byte[segment.remaining()];
            segment.get(segmentBytes);
            baos.write(segmentBytes);
        }
        byte[] serializedCapnp = baos.toByteArray();
        ByteBuffer byteBuffer = ByteBuffer.allocate(serializedCapnp.length);
        byteBuffer.put(serializedCapnp);
        byteBuffer.flip();
        MessageReader messageReader = Serialize.read(byteBuffer);
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
            Stock stockOne = new Stock(100.00, 5_000L, "CFLT", "NASDAQ", io.confluent.developer.TxnType.BUY);
            Stock stockTwo = new Stock(500.00, 105_000L, "AAPL", "NASDAQ", io.confluent.developer.TxnType.BUY);

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
       StockAvro stockAvro = stockAvro();
       Stock stock = javaRecordStock();
       StockTradeEncoder stockTradeEncoder = sbeRecordSupplier.get();
       byte[] protoSerialized = protobufSerializer.serialize("topic", stockProto);
       byte[] serializedAvro = avroSerializer.serialize("topic", stockAvro);
       byte[] sbeBytes =  sbeSerializer.serialize("topic", stockTradeEncoder);
       byte[] kryoBytes =  kryoSerializer.serialize("topic", stock);
       byte[] jacksonBytes = jacksonRecordSerializer.serialize("topic", stock);

       assertEquals(27, protoSerialized.length);
       assertEquals(23, serializedAvro.length);
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
        assertEquals(price, stockTradeDecoder.price());
        assertEquals(shares, stockTradeDecoder.shares());
        assertEquals("CFLT", stockTradeDecoder.symbol());
        assertEquals(Exchange.NASDAQ, stockTradeDecoder.exchange());
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
    void sbeDirectEncodeDecodeTest() {
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
        return new Stock(101.1, 70_000L, "CFLT", "NASDAQ", io.confluent.developer.TxnType.BUY);
    }

    StockAvro stockAvro() {
        StockAvro.Builder builder = StockAvro.newBuilder();
        builder.setType(io.confluent.developer.avro.TxnType.BUY);
        builder.setPrice(101.0);
        builder.setShares(70_000);
        builder.setSymbol("CFLT");
        builder.setExchange(io.confluent.developer.avro.Exchange.NASDAQ);
        return builder.build();
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

}
