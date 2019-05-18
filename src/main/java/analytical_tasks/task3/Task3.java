package analytical_tasks.task3;

import kafka.EventDeserializer;
import kafka.TupleSerializationSchema;
import main.Main;
import model.CommentEvent;
import model.LikeEvent;
import model.PostEvent;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static main.Main.setGlobalConfig;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;

public class Task3 {

    // Initialize Logger
    private static Logger logger = LoggerFactory.getLogger(Task3.class);

    private static final String DEFAULT_CONFIG_LOCATION = "config.properties";
    private static final String kafkaBrokerList = "localhost:9092";


    public static void main(String[] args) throws Exception {

        logger.info(String.format("Setting up configuration using config location: %s.", DEFAULT_CONFIG_LOCATION));
        setGlobalConfig(DEFAULT_CONFIG_LOCATION);
        org.apache.commons.configuration2.Configuration configs = Main.getGlobalConfig();
        assert configs != null;

        int maxDelay = configs.getInt("maxDelayInSec");
        logger.info(String.format("Maximum delay for the source: %d.", maxDelay));

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", kafkaBrokerList);
        kafkaProps.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaProps.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");


        // Source for all comment events
        DataStream<CommentEvent> commentEventsSource = env
                .addSource(new FlinkKafkaConsumer011<>("comments", new EventDeserializer<>(CommentEvent.class), kafkaProps))
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<CommentEvent>(Time.seconds(maxDelay)) {
                            @Override
                            public long extractTimestamp(CommentEvent element) {
                                return element.getTimeMilisecond();
                            }
                        });

        // DataStream for likes
        // Going to be used for user engagement and active post tracking
        DataStream<LikeEvent> likeEventsSource = env
                .addSource(new FlinkKafkaConsumer011<>("likes", new EventDeserializer<>(LikeEvent.class), kafkaProps))
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<LikeEvent>(Time.seconds(maxDelay)) {
                            @Override
                            public long extractTimestamp(LikeEvent element) {
                                return element.getTimeMilisecond();
                            }
                        });

        // DataStream for posts
        // Going to be used for user engagement and active post tracking
        DataStream<PostEvent> postsEventSource = env
                .addSource(new FlinkKafkaConsumer011<>("posts", new EventDeserializer<>(PostEvent.class), kafkaProps))
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<PostEvent>(Time.seconds(maxDelay)) {
                            @Override
                            public long extractTimestamp(PostEvent element) {
                                return element.getTimeMilisecond();
                            }
                        });


        // (PersonId, FeatureID, FeatureValue, Time)
        DataStream<Tuple4<String, Integer, Double, Long>> postFeatures = postsEventSource
                .keyBy((KeySelector<PostEvent, String>) PostEvent::getPersonId)
                .process(new Task3_PostsMetricsProcess());

        // (PersonId, FeatureID, FeatureValue, Time)
        DataStream<Tuple4<String, Integer, Double, Long>> commentFeatures = commentEventsSource
                .keyBy((KeySelector<CommentEvent, String>) CommentEvent::getPersonId)
                .process(new Task3_CommentsMetricsProcess());

        MapStateDescriptor<String, PostEvent> postBroadcastStateDescriptor = new MapStateDescriptor<>(
                "PostsBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<PostEvent>() {
                }));

        BroadcastStream<PostEvent> postsBroadcastStream = postsEventSource
                .broadcast(postBroadcastStateDescriptor);

        DataStream<Tuple4<String, Integer, Double, Long>> likeEventStat =

                likeEventsSource.keyBy((KeySelector<LikeEvent, String>) LikeEvent::getPersonId)
                        .connect(postsBroadcastStream)
                        .process(new Task3_LikesMetricsProcess());

        // (PersonID, featureID, FeatureValue (User), FeatureValue (Current Mean))
        DataStream<Tuple5<String, Integer, Double, Double, Long>> outlierDetection = postFeatures
                .union(commentFeatures)
                .union(likeEventStat)
                // Partition data by features
                .keyBy(1)
                .process(new Task3_OutlierDetectionProcess());


        outlierDetection.print();

        FlinkKafkaProducer011<Tuple5<String, Integer, Double, Double, Long>> fradulentUserDetectionProducer =
                new FlinkKafkaProducer011<>(
                kafkaBrokerList, // broker list
                "fradulentUserDetection",
                new TupleSerializationSchema<>());

        outlierDetection.addSink(fradulentUserDetectionProducer);
        env.execute("Post Kafka Consumer");

    }

}
