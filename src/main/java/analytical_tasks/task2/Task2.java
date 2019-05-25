package analytical_tasks.task2;

import analytical_tasks.task2.process.CommentSamePostWindowFunction;
import analytical_tasks.task2.process.PostToSameForumWindowFunction;
import analytical_tasks.task2.process.SameLikeProcessWindowFunction;
import kafka.EventDeserializer;
import main.Main;
import model.CommentEvent;
import model.LikeEvent;
import model.PostEvent;
import org.apache.commons.configuration2.Configuration;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple12;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import preparation.ReorderProcess;

import java.util.LinkedList;
import java.util.Properties;
import java.util.function.Consumer;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;

public class Task2 {

    //Keeps track of the static scores that are merged at the end.
    private static ScoreHandler[] staticScores = new ScoreHandler[10];

    // Main class for Task 2.
    public static void main(String[] args) throws Exception {
        Main.setGlobalConfig(Main.DEFAULT_CONFIG_LOCATION);
        Configuration configuration = Main.getGlobalConfig();
        // Read static data and calculate static score.
        StaticScoreCalculator staticScoreCalculator = new StaticScoreCalculator();
        try {
            staticScores = staticScoreCalculator.readStaticScores();
        } catch (Exception e) {
            System.out.println("Static Score Calculation failed.");
            e.printStackTrace();
            return;
        }

        int maxDelay = configuration.getInt("maxDelayInSec");

        // Read dynamic data, calculate stream score.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9092");

        // Always read the Kafka topic from the start
        kafkaProps.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");

        // So that we do not need to write info to Kafka again and again
        kafkaProps.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");

        String[] selectedUserIdArray = new String[10];
        for(int i = 0; i < 10; i ++) {
            selectedUserIdArray[i] = configuration.getString("friendId" + i);
        }

        // Source for all comment events
        DataStream<LikeEvent> likeEventDataStream = env.addSource(
                new FlinkKafkaConsumer011<>("likes", new EventDeserializer<>(LikeEvent.class), kafkaProps));

        // This calculates the score when the users like the same post
        DataStream<ScoreHandler[]> likeEventProcessedStream = likeEventDataStream
                .process(new ReorderProcess<>()).setParallelism(1)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LikeEvent>(Time.seconds(maxDelay)) {
                    @Override
                    public long extractTimestamp(LikeEvent likeEvent) {
                        return likeEvent.getTimeMilisecond();
                    }
                })
                .keyBy((KeySelector<LikeEvent, String>) LikeEvent::getPostId)
                .window(SlidingEventTimeWindows.of(Time.hours(4), Time.hours(1)))
                .process(new SameLikeProcessWindowFunction(selectedUserIdArray));

        //Source for same forum post
        DataStream<PostEvent> postEventDataStream = env.addSource(
                new FlinkKafkaConsumer011<>("posts", new EventDeserializer<>(PostEvent.class), kafkaProps));

        // This increases the score when they post on the same forum
        DataStream<ScoreHandler[]> postEventProcessedStream = postEventDataStream
                .process(new ReorderProcess<>()).setParallelism(1)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<PostEvent>(Time.seconds(10)) {
                    @Override
                    public long extractTimestamp(PostEvent postEvent) {
                        return postEvent.getTimeMilisecond();
                    }
                })
                .keyBy((KeySelector<PostEvent, String>) PostEvent::getId)
                .window(SlidingEventTimeWindows.of(Time.hours(4), Time.hours(1)))
                .process(new PostToSameForumWindowFunction(selectedUserIdArray));

        // Source for same Comment
        DataStream<CommentEvent> commentEventDataStream = env.addSource(
                new FlinkKafkaConsumer011<>("comments", new EventDeserializer<>(CommentEvent.class), kafkaProps));

        // This increases the score if they comment on the same thing.
        DataStream<ScoreHandler[]> commentEventProcessedStream = commentEventDataStream
                .process(new ReorderProcess<>()).setParallelism(1)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<CommentEvent>(Time.seconds(10)) {
                    @Override
                    public long extractTimestamp(CommentEvent commentEvent) {
                        return commentEvent.getTimeMilisecond();
                    }
                })
                .keyBy((KeySelector<CommentEvent, String>) CommentEvent::getId)
                .window(SlidingEventTimeWindows.of(Time.hours(4), Time.hours(1)))
                .process(new CommentSamePostWindowFunction(selectedUserIdArray));


        //Finally we first union all streams
        likeEventProcessedStream.union(postEventProcessedStream)
                .union(commentEventProcessedStream)
                //Then we perform another window
                .windowAll(SlidingEventTimeWindows.of(Time.hours(4), Time.hours(1)))
                .process(new ProcessAllWindowFunction<ScoreHandler[], Object, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<ScoreHandler[]> iterable, Collector<Object> collector) throws Exception {
                        ScoreHandler[] result = new ScoreHandler[10];
                        for(int i = 0; i < 10; i++) {
                            result[i] = new ScoreHandler(selectedUserIdArray[i]);
                        }
                        iterable.forEach(iterScoreHandlerArray -> {
                            for(int i = 0; i < 10; i++) {
                                result[i].merge(iterScoreHandlerArray[i]);
                            }
                        });

                        Tuple12[] friendProposals = new Tuple12[10];
                        for(int i = 0; i < 10; i++) {
                            result[i].merge(staticScores[i]);
                            friendProposals[i] = result[i].returnTop5();
                            friendProposals[i].f0 = context.window().getEnd();

                        }
                    }
                }).setParallelism(1);
        env.execute();

    }
}
