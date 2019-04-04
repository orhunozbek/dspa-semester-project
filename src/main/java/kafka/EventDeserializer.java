package kafka;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class EventDeserializer<T> implements
        DeserializationSchema<T> {

    private Class<T> eventClass;
    private static ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    public EventDeserializer(Class<T> eventClass) {
        this.eventClass = eventClass;
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public T deserialize(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, eventClass);
    }

    @Override
    public boolean isEndOfStream(T inputMessage) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(eventClass);
    }
}