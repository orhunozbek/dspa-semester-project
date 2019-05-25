package test.Task0;

import kafka.EventDeserializer;
import main.Main;
import model.LikeEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.junit.Test;
import preparation.ReorderProcess;

import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;

/**
 * We cant really output everything, because we only output if we consumed
 * all input
 */
public class NoMissingOutputTest {

    private static final String configurationFilePath = "src/main/java/test/Task0/noMissingTestConfig.properties";

    @Test
    public void noMissingOutput() throws Exception {
        Main.setGlobalConfig(configurationFilePath);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9092");

        // always read the Kafka topic from the start
        kafkaProps.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaProps.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");

        int lines = Main.getGlobalConfig().getInt("lines");
        DataStream<LikeEvent> edits = env.addSource(
                new FlinkKafkaConsumer011<>("likes", new EventDeserializer<>(LikeEvent.class), kafkaProps));

        edits
            .process(new ReorderProcess<>()).setParallelism(1)
            .map(new MapFunction<LikeEvent, LikeEvent>() {
                int count = 0;
                @Override
                public LikeEvent map(LikeEvent likeEvent){
                    count = count + 1;
                    if(lines == count) {
                        System.out.println("Test Successful");
                        System.exit(0);
                    }
                    return likeEvent;
                }
            }).setParallelism(1);

        env.execute();
    }
}
