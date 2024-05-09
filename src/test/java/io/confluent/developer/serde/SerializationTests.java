package io.confluent.developer.serde;

import baseline.*;
import io.confluent.developer.avro.StockAvro;
import io.confluent.developer.proto.StockProto;
import io.confluent.developer.supplier.SbeRecordSupplier;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * User: Bill Bejeck
 * Date: 5/3/24
 * Time: 8:45 AM
 */
class SerializationTests {

    private final SbeRecordSupplier sbeRecordSupplier = new SbeRecordSupplier();
    private final KafkaProtobufSerializer<StockProto> protobufSerializer = new KafkaProtobufSerializer<>();
    private final KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer();
    private final SbeDeserializer sbeDeserializer = new SbeDeserializer();
    private final SbeSerializer sbeSerializer = new SbeSerializer();

    @BeforeEach
    void setUp() {
        Map<String,Object> config = new HashMap<>();
        config.put("schema.registry.url", "mock://localhost:8081");
        protobufSerializer.configure(config, false);
        avroSerializer.configure(config, false);
    }
    
    @Test
    void serializedRecordSizesTest() {
       StockProto stockProto = getStockProto();
       StockAvro stockAvro = getStockAvro();
       StockTradeEncoder stockTradeEncoder = sbeRecordSupplier.get();
       byte[] protoSerialized = protobufSerializer.serialize("topic", stockProto);
       byte[] serializedAvro = avroSerializer.serialize("topic", stockAvro);
       byte[] sbeBytes =  sbeSerializer.serialize("topic", stockTradeEncoder);

       assertEquals(27, protoSerialized.length);
       assertEquals(23, serializedAvro.length);
       assertEquals(26, sbeBytes.length);
    }

    @Test
    void sbeEncodeDecodeTest() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
        UnsafeBuffer unsafeBuffer = new UnsafeBuffer(byteBuffer);
        MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
        StockTradeEncoder stockTradeEncoder = new StockTradeEncoder();
        float price = 99.99f;
        int shares = 3_000;
        String symbol = "CFLT";
        Exchange exchange = Exchange.NASDAQ;
        TxnType txnType = TxnType.BUY;
        stockTradeEncoder.wrapAndApplyHeader(unsafeBuffer, 0, messageHeaderEncoder)
                .price(price)
                .shares(shares)
                .symbol(symbol)
                .exchange(exchange)
                .txnType(txnType);

        StockTradeDecoder stockTradeDecoder = sbeDeserializer.deserialize("topic", sbeSerializer.serialize("topic", stockTradeEncoder));
        assertEquals(price, stockTradeDecoder.price());
        assertEquals(shares, stockTradeDecoder.shares());
        assertEquals(symbol, stockTradeDecoder.symbol());
        assertEquals(exchange, stockTradeDecoder.exchange());
        assertEquals(txnType, stockTradeDecoder.txnType());
    }

    StockAvro getStockAvro() {
        StockAvro.Builder builder = StockAvro.newBuilder();
        builder.setType(io.confluent.developer.avro.TxnType.BUY);
        builder.setPrice(101.0);
        builder.setShares(70_000);
        builder.setSymbol("CFLT");
        builder.setExchange(io.confluent.developer.avro.Exchange.NASDAQ);
        return builder.build();
    }

    StockProto getStockProto() {
        StockProto.Builder stockProtoBuilder = StockProto.newBuilder();
        stockProtoBuilder.setPrice(101.0);
        stockProtoBuilder.setShares(70_000);
        stockProtoBuilder.setSymbol("CFLT");
        stockProtoBuilder.setExchange(io.confluent.developer.proto.Exchange.NASDAQ);
        stockProtoBuilder.setTxn(io.confluent.developer.proto.TxnType.BUY);
        return stockProtoBuilder.build();
    }

}
