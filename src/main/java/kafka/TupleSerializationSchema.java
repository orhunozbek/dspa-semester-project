package kafka;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;

public class TupleSerializationSchema<T extends Tuple> implements SerializationSchema<T> {
    @Override
    public byte[] serialize(T t) {
        return t.toString().getBytes();
    }
}
