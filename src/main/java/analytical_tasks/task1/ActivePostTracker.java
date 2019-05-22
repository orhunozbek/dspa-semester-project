package analytical_tasks.task1;

import kafka.EventDeserializer;
import model.CommentEvent;
import model.LikeEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;

public class ActivePostTracker {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9092");

        // always read the Kafka topic from the start
        kafkaProps.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaProps.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");

        DataStream<CommentEvent> commentEventsSource = env.addSource(
                new FlinkKafkaConsumer011<>("comments", new EventDeserializer<>(CommentEvent.class), kafkaProps));

        DataStream<Tuple2<String, Integer>> commentEvents = commentEventsSource
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<CommentEvent>(Time.seconds(10)) {

                    @Override
                    public long extractTimestamp(CommentEvent element) {
                        return element.getTimeMilisecond();
                    }
                })
                .map(new MapFunction<CommentEvent, Tuple2<String,
                        Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(CommentEvent event) {
                        return new Tuple2<>(
                                event.getReply_to_postId(),1);
                    }

                });

        DataStream<LikeEvent> likeEventsSource = env.addSource(
                new FlinkKafkaConsumer011<>("likes", new EventDeserializer<>(LikeEvent.class), kafkaProps));

        DataStream<Tuple2<String, Integer>> likeEvents = likeEventsSource
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LikeEvent>(Time.seconds(10)) {

                    @Override
                    public long extractTimestamp(LikeEvent element) {
                        return element.getTimeMilisecond();
                    }
                })
                .map(new MapFunction<LikeEvent, Tuple2<String,
                        Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(LikeEvent event) {
                        return new Tuple2<>(
                                event.getPostId(),1);
                    }

                });


        DataStream<Tuple2<String, Integer>> activePosts =
                likeEvents.union(commentEvents)
                .keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.hours(12), Time.minutes(30)))
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
                        return new Tuple2<String, Integer> (stringIntegerTuple2.f0, stringIntegerTuple2.f1+t1.f1);
                    }
                });

        activePosts.print();
        env.execute("Post Kafka Consumer");
    }

}
