package kafka;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class EventSerializer<T> implements Serializer<T> {

    private ObjectMapper objectMapper;
    private Logger logger = LoggerFactory.getLogger(EventSerializer.class);


    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, T t) {
        if(objectMapper == null) {
            objectMapper = new ObjectMapper()
                    .registerModule(new JavaTimeModule());
        }
        try {
            String str = objectMapper.writeValueAsString(t);
            return str.getBytes();
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            logger.error("Failed to parse JSON", e);
        }
        return new byte[0];
    }

    @Override
    public void close() {

    }
}