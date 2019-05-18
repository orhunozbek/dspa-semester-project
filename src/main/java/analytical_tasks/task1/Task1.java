package analytical_tasks.task1;

import kafka.EventDeserializer;
import kafka.TupleSerializationSchema;
import main.Main;
import model.CommentEvent;
import model.LikeEvent;
import org.apache.commons.collections.map.LRUMap;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import preparation.ReorderProcess;

import java.util.*;

import static main.Main.setGlobalConfig;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class Task1 {

    // Initialize Logger
    private static Logger logger = LoggerFactory.getLogger(Task1.class);

    private static final String DEFAULT_CONFIG_LOCATION = "config.properties";
    private static final String kafkaBrokerList = "localhost:9092";

    public static void main(String[] args) throws Exception {

        // Setup configurations
        logger.info(String.format("Setting up configuration using config location: %s.",DEFAULT_CONFIG_LOCATION));
        setGlobalConfig(DEFAULT_CONFIG_LOCATION);
        org.apache.commons.configuration2.Configuration configs = Main.getGlobalConfig();
        assert configs != null;

        int maxDelay = configs.getInt("maxDelayInSec");
        logger.info(String.format("Maximum delay for the source: %d.",maxDelay));

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
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LikeEvent>(Time.seconds(10)) {

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
                    }
                    else {
                        output.add("withPostID");
                    }
                    return output;
                });

        // Replies to a comment
        DataStream<CommentEvent> commentEventsWithoutPostId = commentEvents.select("withoutPostID");

        // Replies to a post, partitioned by PostID
        KeyedStream<CommentEvent, String> postPartitionedCommentEvents = commentEvents.select("withPostID")
                .keyBy((KeySelector<CommentEvent, String>) CommentEvent::getReply_to_postId);


        // Broadcast state for CommentEvents without postId, in the end, all CommentEvents withoutPostId will be mapped
        // to their parent postId.
        // This pattern is adapted from Flink documentation.
        // https://ci.apache.org/projects/flink/flink-docs-release-1.8/dev/stream/state/broadcast_state.html
        MapStateDescriptor<String, CommentEvent> commentBroadcastStateDescriptor = new MapStateDescriptor<>(
                "CommentsBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<CommentEvent>() {}));


        BroadcastStream<CommentEvent> commentWithoutIdBroadcastStream = commentEventsWithoutPostId
                .broadcast(commentBroadcastStateDescriptor);


        DataStream<CommentEvent> replies = postPartitionedCommentEvents
                .connect(commentWithoutIdBroadcastStream)
                .process(
                        new KeyedBroadcastProcessFunction<String, CommentEvent, CommentEvent, CommentEvent>() {

                            private String postID;
                            private HashSet<String> equivalenceClass = new HashSet<String>();
                            private LRUMap cache = new LRUMap(100);


                            @Override
                            public void processElement(CommentEvent commentEvent, ReadOnlyContext readOnlyContext, Collector<CommentEvent> collector) throws Exception {
                                if (postID == null) postID = readOnlyContext.getCurrentKey();
                                addToMapState(commentEvent, collector);
                                readOnlyContext.getCurrentKey();
                            }

                            private void addToMapState(CommentEvent commentEvent, Collector<CommentEvent> collector){

                                equivalenceClass.add(commentEvent.getId());
                                collector.collect(commentEvent);

                                Object arr = cache.get(commentEvent.getId());

                                if(arr!= null) {
                                    for (CommentEvent event : (ArrayList<CommentEvent>) arr) {

                                        CommentEvent updatedCommentEvent =
                                                new CommentEvent(event.getTimeMilisecond(),
                                                        event.getId(),
                                                        event.getPersonId(),
                                                        event.getCreationDate(),
                                                        event.getLocationIP(),
                                                        event.getBrowserUsed(),
                                                        event.getContent(),
                                                        commentEvent.getReply_to_postId(),
                                                        event.getReply_to_commentId(),
                                                        event.getPlaceId());

                                        addToMapState(updatedCommentEvent, collector);
                                    }
                                }
                            }

                            @Override
                            public void processBroadcastElement(CommentEvent commentEvent, Context context, Collector<CommentEvent> collector) throws Exception {

                                if (equivalenceClass.contains(commentEvent.getReply_to_commentId())){

                                    equivalenceClass.add(commentEvent.getId());

                                    CommentEvent updatedCommentEvent =
                                            new CommentEvent(commentEvent.getTimeMilisecond(),
                                                    commentEvent.getId(),
                                                    commentEvent.getPersonId(),
                                                    commentEvent.getCreationDate(),
                                                    commentEvent.getLocationIP(),
                                                    commentEvent.getBrowserUsed(),
                                                    commentEvent.getContent(),
                                                    postID,
                                                    commentEvent.getReply_to_commentId(),
                                                    commentEvent.getPlaceId());

                                    addToMapState(updatedCommentEvent, collector);

                                }

                                else{

                                    if(cache.get(commentEvent.getReply_to_commentId()) == null){

                                        ArrayList<CommentEvent> arr = new ArrayList<>();
                                        arr.add(commentEvent);
                                        cache.put(commentEvent.getReply_to_commentId(),arr);
                                    }

                                    else{

                                        ArrayList<CommentEvent> arr =
                                                (ArrayList<CommentEvent>) cache.get(commentEvent.getReply_to_commentId());

                                        arr.add(commentEvent);
                                        cache.put(commentEvent.getReply_to_commentId(),arr);
                                    }
                                }
                            }
                        }
                );


        DataStream <Tuple4<Long, Long, String, Integer>> repliesCounts = replies
                .map((MapFunction<CommentEvent, Tuple2<String, Integer>>) commentEvent -> new Tuple2<>(commentEvent.getReply_to_postId(),1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.hours(12), Time.minutes(30)))
                .process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple4<Long, Long, String, Integer>, Tuple, TimeWindow>() {

                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple4<Long, Long, String, Integer>> collector) throws Exception {

                        int count = 0;
                        String key = null;

                        for (Tuple2<String, Integer> event:iterable){
                            if (key == null) key = event.f0;
                            count ++;
                        }

                        collector.collect( new Tuple4<>(context.window().getStart(), context.window().getEnd(), key, count));
                    }
                });

        DataStream <Tuple4<Long, Long, String, Integer>> commentsCounts = commentEvents.select("withPostID")
                .map((MapFunction<CommentEvent, Tuple2<String, Integer>>) commentEvent -> new Tuple2<String, Integer>(commentEvent.getReply_to_postId(),1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.hours(12), Time.minutes(30)))
                .process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple4<Long, Long, String, Integer>, Tuple, TimeWindow>() {

                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple4<Long, Long, String, Integer>> collector) throws Exception {

                        int count = 0;
                        String key = null;

                        for (Tuple2<String, Integer> event:iterable){
                            if (key == null) key = event.f0;
                            count ++;
                        }

                        collector.collect( new Tuple4<>(context.window().getStart(), context.window().getEnd(), key, count));
                    }
                });



        // Part 3: Calculation for Unique User Engagement for Output

        // Map LikeEvents and CommentEvents into a common representation.
        DataStream<Tuple3<String, String, Integer>> userLikesPost = likeEvents
                .map((MapFunction<LikeEvent, Tuple3<String, String, Integer>>) likeEvent
                        -> Tuple3.of(likeEvent.getPostId(), likeEvent.getPersonId(),1))
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.INT));

        DataStream<Tuple3<String, String, Integer>> userRepliesPost = replies
                .map((MapFunction<CommentEvent, Tuple3<String, String, Integer>>) commentEvent
                        -> Tuple3.of(commentEvent.getReply_to_postId(), commentEvent.getPersonId(),1))
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.INT));

        // Create a union between replies and likes to calculate unique user engagement per window
        DataStream <Tuple4<Long, Long, String, Integer>>  uniqueUserEngagementCounts = userLikesPost
                .union(userRepliesPost)
                .keyBy(0)
                .flatMap(new RichFlatMapFunction<Tuple3<String, String, Integer>, Tuple2<String, Integer>>() {

                    private transient ValueState<HashSet<String>> engagedUsers;

                    @Override
                    public void flatMap(Tuple3<String, String, Integer> tupl, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        HashSet<String> currentEngagedUsers = engagedUsers.value();

                        if (!currentEngagedUsers.contains(tupl.f1)){
                            currentEngagedUsers.add(tupl.f1);
                            engagedUsers.update(currentEngagedUsers);

                            collector.collect(new Tuple2<>(tupl.f0,1));
                        }
                    }

                    @Override
                    public void open(Configuration config) {
                        ValueStateDescriptor<HashSet<String>> descriptor =
                                new ValueStateDescriptor<>(
                                        "engagedUsers", // the state name
                                        TypeInformation.of(new TypeHint<HashSet<String>>() {}), // type information
                                        new HashSet<String>()); // default value of the state, if nothing was set
                        engagedUsers = getRuntimeContext().getState(descriptor);
                    }

                }).keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.hours(12), Time.minutes(30)))
                .process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple4<Long, Long, String, Integer>, Tuple, TimeWindow>() {

                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple4<Long, Long, String, Integer>> collector) throws Exception {

                        int count = 0;
                        String key = null;

                        for (Tuple2<String, Integer> event:iterable){
                            if (key == null) key = event.f0;
                            count ++;
                        }

                        collector.collect( new Tuple4<>(context.window().getStart(), context.window().getEnd(), key, count));
                    }
                });


        FlinkKafkaProducer011<Tuple4<Long, Long, String, Integer>> userEngagementProducer = new FlinkKafkaProducer011<>(
                kafkaBrokerList, // broker list
                "userEngagementCounts",
                new TupleSerializationSchema<>());

        FlinkKafkaProducer011<Tuple4<Long, Long, String, Integer>> commentsProducer = new FlinkKafkaProducer011<>(
                kafkaBrokerList, // broker list
                "commentsCounts",
                new TupleSerializationSchema<>());

        FlinkKafkaProducer011<Tuple4<Long, Long, String, Integer>> repliesProducer = new FlinkKafkaProducer011<>(
                kafkaBrokerList, // broker list
                "repliesCounts",
                new TupleSerializationSchema<>());


        uniqueUserEngagementCounts.addSink(userEngagementProducer);
        commentsCounts.addSink(commentsProducer);
        repliesCounts.addSink(repliesProducer);

        uniqueUserEngagementCounts.print();
        env.execute("Post Kafka Consumer");

    }

}
