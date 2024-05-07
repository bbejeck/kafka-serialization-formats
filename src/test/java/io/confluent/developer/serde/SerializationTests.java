package io.confluent.developer.serde;

import baseline.MessageHeaderDecoder;
import baseline.StockTradeDecoder;
import baseline.TxnType;
import io.confluent.developer.avro.StockAvro;
import io.confluent.developer.avro.txn;
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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * User: Bill Bejeck
 * Date: 5/3/24
 * Time: 8:45 AM
 */
public class SerializationTests {

    private  SbeRecordSupplier sbeRecordSupplier = new SbeRecordSupplier();
    private KafkaProtobufSerializer<StockProto> protobufSerializer = new KafkaProtobufSerializer<>();
    private KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer();

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
       byte[] protoSerialized = protobufSerializer.serialize("topic", stockProto);
       byte[] serializedAvro = avroSerializer.serialize("topic", stockAvro);
       byte[] sbeBytes = sbeRecordSupplier.get();

      System.out.printf("Proto bytes %d%n", protoSerialized.length);
      System.out.printf("Avro bytes %d%n", serializedAvro.length);
      System.out.printf("SBE bytes %d%n", sbeBytes.length);
    }

    @Test
    void sbeEncodeDecodeTest() {
        byte[] sbeBytes = sbeRecordSupplier.get();
        UnsafeBuffer unsafeBuffer = new UnsafeBuffer(ByteBuffer.wrap(sbeBytes));
        MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        StockTradeDecoder stockTradeDecoder = new StockTradeDecoder();
        stockTradeDecoder.wrapAndApplyHeader(unsafeBuffer, 0, messageHeaderDecoder);
        assertTrue(stockTradeDecoder.price() >= 1);
        assertTrue(stockTradeDecoder.shares() > 10);
        assertNotNull(stockTradeDecoder.symbol());
        assertNotNull(stockTradeDecoder.exchange().name());
        assertTrue(stockTradeDecoder.txnType() == TxnType.BUY || stockTradeDecoder.txnType() == TxnType.SELL);
    }

    StockAvro getStockAvro() {
        StockAvro.Builder builder = StockAvro.newBuilder();
        builder.setTxnType(txn.BUY);
        builder.setPrice(101.0);
        builder.setShares(70_000);
        builder.setSymbol("CFLT");
        builder.setExchange("NASDQ");
        return builder.build();
    }

    StockProto getStockProto() {
        StockProto.Builder stockProtoBuilder = StockProto.newBuilder();
        stockProtoBuilder.setPrice(101.0);
        stockProtoBuilder.setShares(70_000);
        stockProtoBuilder.setSymbol("CFLT");
        stockProtoBuilder.setExchange("NASDQ");
        stockProtoBuilder.setTxn(io.confluent.developer.proto.TxnType.BUY);
        return stockProtoBuilder.build();
    }

}
