package io.confluent.developer.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.developer.Stock;
import org.apache.kafka.common.serialization.Serializer;

/**
 * User: Bill Bejeck
 * Date: 4/29/24
 * Time: 10:12 AM
 */
public class JacksonRecordSerializer  implements Serializer<Stock> {

    private final ObjectMapper mapper = new ObjectMapper();
    @Override
    public byte[] serialize(String s, Stock stock) {
        if (stock == null) {
            return null;
        }
        try {
            return mapper.writeValueAsBytes(stock);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
