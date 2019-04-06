package thread;

import kafka.EventDeserializer;
import model.PostEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;

public class PostThread extends Thread{

    public void run() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9092");

        // always read the Kafka topic from the start
        kafkaProps.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaProps.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");

        DataStream<PostEvent> edits = env.addSource(
                new FlinkKafkaConsumer011<>("posts", new EventDeserializer<>(PostEvent.class), kafkaProps));

        DataStream<PostEvent> result = edits
                .map(new MapFunction<PostEvent, PostEvent>() {
                    @Override
                    public PostEvent map(PostEvent event) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        return event;
                    }
                });
        result.print();

        try {
            env.execute("Post Kafka Consumer");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
