package kafka;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple;

public class TupleSerializationSchema<T extends Tuple> implements SerializationSchema<T> {
    @Override
    public byte[] serialize(T t) {
        return t.toString().getBytes();
    }
}
