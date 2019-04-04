package template;

import kafka.EventDeserializer;
import model.PostEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class FlinkKafkaEventConsumer {

        public static void main(String[] args) throws Exception {
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            Properties kafkaProps = new Properties();
            kafkaProps.setProperty("zookeeper.connect", "localhost:2181");
            kafkaProps.setProperty("bootstrap.servers", "localhost:9092");

            // always read the Kafka topic from the start
            kafkaProps.setProperty("auto.offset.reset", "earliest");


            DataStream<PostEvent> edits = env.addSource(
                    new FlinkKafkaConsumer011<>("posts", new EventDeserializer<>(PostEvent.class), kafkaProps));

            DataStream<String> result = edits
                    .map(new MapFunction<PostEvent, String>() {
                        @Override
                        public String map(PostEvent event) {
                            return event.getContent();
                        }

                    });

            result.print();
            env.execute("Post Kafka Consumer");
        }

}
