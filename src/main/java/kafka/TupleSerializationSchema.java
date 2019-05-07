package kafka;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class TupleSerializationSchema implements SerializationSchema<Tuple4<Long, Long, String, Integer>> {

    @Override
    public byte[] serialize(Tuple4<Long, Long, String, Integer> longLongStringIntegerTuple4) {
        return longLongStringIntegerTuple4.toString().getBytes();
    }
}
