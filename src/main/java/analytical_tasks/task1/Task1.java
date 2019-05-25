package analytical_tasks.task1;

import kafka.EventDeserializer;
import kafka.TupleSerializationSchema;
import main.Main;
import model.CommentEvent;
import model.LikeEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static main.Main.setGlobalConfig;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class Task1 {

    // Initialize Logger
    private static Logger logger = LoggerFactory.getLogger(Task1.class);

    private static final String DEFAULT_CONFIG_LOCATION = "config.properties";
    private static final String kafkaBrokerList = "localhost:9092";
    private static final String DEFAULT_OUTPUT_KAFKA_TOPIC_NAME = "activityCounter";

    public static void main(String[] args) throws Exception {

        String configLocation = DEFAULT_CONFIG_LOCATION;
        String outputKafkaTopicName = DEFAULT_OUTPUT_KAFKA_TOPIC_NAME;

        if (args.length>0) {
            configLocation = args[0];
            outputKafkaTopicName = args[1];
        }

        // Setup configurations
        logger.info(String.format("Setting up configuration using config location: %s.", configLocation));
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
        DataStream<CommentEvent> commentEventsSource = env.addSource(
                new FlinkKafkaConsumer011<>("comments", new EventDeserializer<>(CommentEvent.class), kafkaProps));

        // DataStream for likes
        // Going to be used for user engagement and active post tracking
        DataStream<LikeEvent> likeEventsSource = env.addSource(
                new FlinkKafkaConsumer011<>("likes", new EventDeserializer<>(LikeEvent.class), kafkaProps));


        DataStream<LikeEvent> likeEvents = likeEventsSource
                //.process(new ReorderProcess<LikeEvent>()).setParallelism(1)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LikeEvent>(Time.seconds(maxDelay)) {
                    @Override
                    public long extractTimestamp(LikeEvent element) {
                        return element.getTimeMilisecond();
                    }
                });

        SplitStream<CommentEvent> commentEvents = commentEventsSource
                //.process(new ReorderProcess<CommentEvent>()).setParallelism(1)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<CommentEvent>(Time.seconds(maxDelay)) {

                    @Override
                    public long extractTimestamp(CommentEvent element) {
                        return element.getTimeMilisecond();
                    }
                })
                .split((OutputSelector<CommentEvent>) commentEvent -> {
                    List<String> output = new ArrayList<String>();
                    if (commentEvent.getReply_to_postId().equals("")) {
                        output.add("withoutPostID");
                    } else {
                        output.add("withPostID");
                    }
                    return output;
                });



        MapStateDescriptor<String, CommentEvent> commentBroadcastStateDescriptor = new MapStateDescriptor<>(
                "CommentsBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<CommentEvent>() {
                }));

        BroadcastStream<CommentEvent> commentWithoutIdBroadcastStream = commentEvents
                .select("withoutPostID")
                .broadcast(commentBroadcastStateDescriptor);


        DataStream<CommentEvent> replies = commentEvents.select("withPostID")
                .keyBy((KeySelector<CommentEvent, String>) CommentEvent::getReply_to_postId)
                .connect(commentWithoutIdBroadcastStream)
                .process(new  Task1_CommentResolutionProcess());

        DataStream<Tuple4<String,Integer,String, Long>> repliesSimplified = replies.map(new MapFunction<CommentEvent, Tuple4<String,Integer,String, Long>>() {
            @Override
            public Tuple4<String,Integer,String, Long> map(CommentEvent commentEvent) throws Exception {
                int reply_or_comment = commentEvent.getReply_to_commentId().equals("") ? 1:0;
                return new Tuple4<>(commentEvent.getReply_to_postId(),reply_or_comment,commentEvent.getPersonId(), commentEvent.getTimeMilisecond());
            }
        });

        DataStream<Tuple4<String,Integer,String, Long>> likesSimplified = likeEvents.map(new MapFunction<LikeEvent, Tuple4<String, Integer, String, Long>>() {
            @Override
            public Tuple4<String, Integer, String, Long> map(LikeEvent likeEvent) throws Exception {
                return new Tuple4<>(likeEvent.getPostId(),2,likeEvent.getPersonId(), likeEvent.getTimeMilisecond());
            }
        });


        // PostID, EventType, PersonID, Timestamp --> Input
        // Timestamp, PostID, EventType, Count --> Output
        // Comment: 0 , Reply: 1 , Like: 2
        DataStream<Tuple4<Long, String, String, Integer>> resultStream = likesSimplified
                .union(repliesSimplified)
                .keyBy(0)
                .process(new Task1_CounterKeyedProcessFunction());

        DataStream<Tuple2<Long, Iterable<Tuple3<String, String, Integer>>>> windowedResultStream =
                resultStream.windowAll(TumblingEventTimeWindows.of(Time.minutes(30)))
                .process(new ProcessAllWindowFunction<Tuple4<Long, String, String, Integer>,
                        Tuple2<Long, Iterable<Tuple3<String, String, Integer>>>, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<Tuple4<Long, String, String, Integer>> iterable,
                                        Collector<Tuple2<Long, Iterable<Tuple3<String, String, Integer>>>> collector) throws Exception {

                        long timeWindow = context.window().getStart();
                        ArrayList<Tuple3<String, String, Integer>> resArray = new ArrayList<>();

                        for(Tuple4<Long, String, String, Integer> update : iterable){
                            if (update.f0.equals(timeWindow)) resArray.add(new Tuple3<>(update.f1, update.f2, update.f3));
                        }

                        collector.collect( new Tuple2<>(context.window().getEnd(), resArray));
                    }
                });


        FlinkKafkaProducer011<Tuple2<Long, Iterable<Tuple3<String, String, Integer>>>> windowedResultStreamProducer =
                new FlinkKafkaProducer011<>(
                        kafkaBrokerList, // broker list
                        outputKafkaTopicName,
                        new TupleSerializationSchema<>());

        windowedResultStream.addSink(windowedResultStreamProducer);
        env.execute("Post Kafka Consumer");

    }

}
