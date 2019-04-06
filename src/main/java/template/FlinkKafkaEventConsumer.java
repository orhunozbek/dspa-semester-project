package template;

import kafka.EventDeserializer;
import model.CommentEvent;
import model.PostEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import javax.xml.stream.events.Comment;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;

public class FlinkKafkaEventConsumer {

        public static void main(String[] args) throws Exception {
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            Properties kafkaProps = new Properties();
            kafkaProps.setProperty("bootstrap.servers", "localhost:9092");

            // always read the Kafka topic from the start
            kafkaProps.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
            kafkaProps.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");

            DataStream<CommentEvent> edits = env.addSource(
                    new FlinkKafkaConsumer011<>("comments", new EventDeserializer<>(CommentEvent.class), kafkaProps));

            DataStream<CommentEvent> result = edits
                    .map(new MapFunction<CommentEvent, CommentEvent>() {
                        @Override
                        public CommentEvent map(CommentEvent event) {
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            return event;
                        }
                    });

            // result.print();
            env.execute("Post Kafka Consumer");
        }

}
