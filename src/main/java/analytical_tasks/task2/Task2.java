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
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import preparation.ReorderProcess;

import java.util.LinkedList;
import java.util.Properties;

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
        selectedUserIdArray[0] = configuration.getString("friendId0");
        selectedUserIdArray[1] = configuration.getString("friendId1");
        selectedUserIdArray[2] = configuration.getString("friendId2");
        selectedUserIdArray[3] = configuration.getString("friendId3");
        selectedUserIdArray[4] = configuration.getString("friendId4");
        selectedUserIdArray[5] = configuration.getString("friendId5");
        selectedUserIdArray[6] = configuration.getString("friendId6");
        selectedUserIdArray[7] = configuration.getString("friendId7");
        selectedUserIdArray[8] = configuration.getString("friendId8");
        selectedUserIdArray[9] = configuration.getString("friendId9");


        // Source for all comment events
        DataStream<LikeEvent> likeEventDataStream = env.addSource(
                new FlinkKafkaConsumer011<>("likes", new EventDeserializer<>(LikeEvent.class), kafkaProps));

        // This calculates the score when the users like the same post
        DataStream<ScoreHandler[]> likeEventProcessedStream = likeEventDataStream
                .process(new ReorderProcess<>()).setParallelism(1)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LikeEvent>(Time.seconds(10)) {
                    @Override
                    public long extractTimestamp(LikeEvent likeEvent) {
                        return likeEvent.getTimeMilisecond();
                    }
                })
                .keyBy(new KeySelector<LikeEvent, String>() {
                    @Override
                    public String getKey(LikeEvent likeEvent) throws Exception {
                        return likeEvent.getPostId();
                    }
                })
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
                .keyBy(new KeySelector<PostEvent, String>() {
                    @Override
                    public String getKey(PostEvent postEvent) throws Exception {
                        return postEvent.getId();
                    }
                })
                .window(SlidingEventTimeWindows.of(Time.hours(4), Time.hours(1)))
                .process(new PostToSameForumWindowFunction(selectedUserIdArray));

        // Source for same Comment
        DataStream<CommentEvent> commentEventDataStream = env.addSource(
                new FlinkKafkaConsumer011<CommentEvent>("comments", new EventDeserializer<>(CommentEvent.class), kafkaProps));

        // This increases the score if they comment on the same thing.
        DataStream<ScoreHandler[]> commentEventProcessedStream = commentEventDataStream
                .process(new ReorderProcess<>()).setParallelism(1)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<CommentEvent>(Time.seconds(10)) {
                    @Override
                    public long extractTimestamp(CommentEvent commentEvent) {
                        return commentEvent.getTimeMilisecond();
                    }
                })
                .keyBy(new KeySelector<CommentEvent, String>() {
                    @Override
                    public String getKey(CommentEvent commentEvent) throws Exception {
                        return commentEvent.getId();
                    }
                })
                .window(SlidingEventTimeWindows.of(Time.hours(4), Time.hours(1)))
                .process(new CommentSamePostWindowFunction(selectedUserIdArray));


        //Finally we first union all streams
        likeEventProcessedStream.union(postEventProcessedStream)
                .union(commentEventProcessedStream)
                //Then we perform another window
                .windowAll(SlidingEventTimeWindows.of(Time.hours(4), Time.hours(1)))
                //Aggregate all scores.
                .aggregate(new AggregateFunction<ScoreHandler[], ScoreHandler[], ScoreHandler[]>() {
                    @Override
                    public ScoreHandler[] createAccumulator() {
                        ScoreHandler[] accs = new ScoreHandler[10];
                        for(int i = 0; i < 10; i++) {
                            accs[i] = new ScoreHandler(selectedUserIdArray[i]);
                        }
                        return accs;
                    }

                    @Override
                    public ScoreHandler[] add(ScoreHandler[] scoreHandler1, ScoreHandler[] scoreHandler2) {
                        for(int i = 0; i < 10; i++) {
                            scoreHandler1[i].merge(scoreHandler2[i]);
                        }
                        return scoreHandler1;
                    }

                    @Override
                    public ScoreHandler[] getResult(ScoreHandler[] scoreHandlers) {
                        return scoreHandlers;
                    }

                    @Override
                    public ScoreHandler[] merge(ScoreHandler[] scoreHandler1, ScoreHandler[] scoreHandler2) {
                        for(int i = 0; i < 10; i++) {
                            scoreHandler1[i].merge(scoreHandler2[i]);
                        }
                        return scoreHandler1;
                    }
                }).setParallelism(1)
                // And finally merge the static scores in and output the top 5
                // friend suggestions
                .map(new MapFunction<ScoreHandler[], LinkedList<String>[]>() {
                    @Override
                    public LinkedList<String>[] map(ScoreHandler[] scoreHandlers) throws Exception {
                        LinkedList<String>[] friendProposals = new LinkedList[10];
                        for(int i = 0; i < 10; i++) {
                            staticScores[i].merge(scoreHandlers[i]);
                            friendProposals[i] = staticScores[i].returnTop5();
                        }
                        return friendProposals;
                    }
                }).setParallelism(1);


        env.execute();

    }
}
