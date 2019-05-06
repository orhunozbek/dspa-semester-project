package analytical_tasks.task1;

import kafka.EventDeserializer;
import main.Main;
import model.CommentEvent;
import model.LikeEvent;
import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import preparation.ReorderProcess;

import javax.xml.stream.events.Comment;
import java.util.*;

import static main.Main.setGlobalConfig;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;

public class Task1 {

    // Initialize Logger
    private static Logger logger = LoggerFactory.getLogger(Task1.class);

    // Configuration
    private static final String DEFAULT_CONFIG_LOCATION = "config.properties";

    public static void main(String[] args) throws Exception {

        // Setup configurations
        setGlobalConfig(DEFAULT_CONFIG_LOCATION);
        org.apache.commons.configuration2.Configuration configs = Main.getGlobalConfig();

        assert configs != null;
        int maxDelay = configs.getInt("maxDelayInSec");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9092");

        // Always read the Kafka topic from the start
        kafkaProps.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");

        // So that we do not need to write info to Kafka again and again
        kafkaProps.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");


        // Source for all comment events
        DataStream<CommentEvent> commentEventsSource = env.addSource(
                new FlinkKafkaConsumer011<>("comments", new EventDeserializer<>(CommentEvent.class), kafkaProps));

        // Split all commentEvents into two:
        // Replies to a post
        // Replies to a comment

        SplitStream<CommentEvent> commentEvents = commentEventsSource
                //.process(new ReorderProcess<CommentEvent>()).setParallelism(1)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<CommentEvent>(Time.seconds(maxDelay)) {

                    @Override
                    public long extractTimestamp(CommentEvent element) {
                        return element.getTimeMilisecond();
                    }
                })
                .split(new OutputSelector<CommentEvent>() {
                    @Override
                    public Iterable<String> select(CommentEvent commentEvent) {
                        List<String> output = new ArrayList<String>();
                        if (commentEvent.getReply_to_postId().equals("")) {
                            output.add("withoutPostID");
                        }
                        else {
                            output.add("withPostID");
                        }
                        return output;
                    }
                });

        // Replies to a comment
        DataStream<CommentEvent> commentEventsWithoutPostId = commentEvents.select("withoutPostID");

        // Replies to a post, partitioned by Key
        KeyedStream<CommentEvent, String> postPartitionedCommentEvents = commentEvents.select("withPostID").keyBy(new KeySelector<CommentEvent, String>() {
            public String getKey(CommentEvent commentEvent) { return commentEvent.getReply_to_postId(); }
        });


        // Broadcast state for CommentEvents without postId.
        MapStateDescriptor<String, CommentEvent> commentBroadcastStateDescriptor = new MapStateDescriptor<>(
                "CommentsBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<CommentEvent>() {}));


        BroadcastStream<CommentEvent> commentWithoutIdBroadcastStream = commentEventsWithoutPostId
                .broadcast(commentBroadcastStateDescriptor);

        // All CommentEvents mapped to a proper postId
        // Used LRU Cache to store CommentEvents which cannot be found in the mapping
        // Used HashMap for Comment -> Post relationships
        DataStream<CommentEvent> replies = postPartitionedCommentEvents
                .connect(commentWithoutIdBroadcastStream)
                .process(
                        new KeyedBroadcastProcessFunction<String, CommentEvent, CommentEvent, CommentEvent>() {

                            private final MapStateDescriptor<String, CommentEvent> commentBroadcastStateDescriptor = new MapStateDescriptor<>(
                                    "CommentsBroadcastState",
                                    BasicTypeInfo.STRING_TYPE_INFO,
                                    TypeInformation.of(new TypeHint<CommentEvent>() {}));


                            private HashMap<String,String> mapState = new HashMap<String, String>();
                            private LRUMap cache = new LRUMap(100);

                            @Override
                            public void onTimer(long timestamp, OnTimerContext ctx, Collector<CommentEvent> out) throws Exception {
                                super.onTimer(timestamp, ctx, out);
                            }

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                            }

                            @Override
                            public void processElement(CommentEvent commentEvent, ReadOnlyContext readOnlyContext, Collector<CommentEvent> collector) throws Exception {
                                addToMapState(commentEvent, collector);
                            }

                            private void addToMapState(CommentEvent commentEvent, Collector<CommentEvent> collector){

                                    mapState.put(commentEvent.getId(),commentEvent.getReply_to_postId());
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
                                // final MapState<String, String>  state = getRuntimeContext().getMapState(mapStateDesc);

                                if (mapState.get(commentEvent.getReply_to_commentId())!=null){

                                    String postId = mapState.get(commentEvent.getReply_to_commentId());
                                    mapState.put(commentEvent.getId(),postId);

                                    CommentEvent updatedCommentEvent =
                                            new CommentEvent(commentEvent.getTimeMilisecond(),
                                                    commentEvent.getId(),
                                                    commentEvent.getPersonId(),
                                                    commentEvent.getCreationDate(),
                                                    commentEvent.getLocationIP(),
                                                    commentEvent.getBrowserUsed(),
                                                    commentEvent.getContent(),
                                                    postId,
                                                    commentEvent.getReply_to_commentId(),
                                                    commentEvent.getPlaceId());

                                    addToMapState(updatedCommentEvent, collector);

                                }

                                else{

                                    if(cache.get(commentEvent.getReply_to_commentId()) == null){

                                        ArrayList<CommentEvent> arr = new ArrayList<>();
                                        arr.add(commentEvent);
                                        cache.put(commentEvent.getReply_to_commentId(),arr);}

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

        // DataStream for likes
        // Going to be used for user engagement and active post tracking
        DataStream<LikeEvent> likeEventsSource = env.addSource(
                new FlinkKafkaConsumer011<>("likes", new EventDeserializer<>(LikeEvent.class), kafkaProps));
                //.process(new ReorderProcess<LikeEvent>()).setParallelism(1);

        DataStream<LikeEvent> likeEvents = likeEventsSource
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LikeEvent>(Time.seconds(10)) {

                    @Override
                    public long extractTimestamp(LikeEvent element) {
                        return element.getTimeMilisecond();
                    }
                });


        //Part 1: Calculation of replies for Output
        DataStream<Iterable<Tuple5<String, Integer, Integer, Integer, Long>>>  repliesCounts = replies.map(new MapFunction<CommentEvent, Tuple5<String, Integer, Integer, Integer, Long>>() {
            @Override
            public Tuple5<String, Integer, Integer, Integer, Long> map(CommentEvent commentEvent) throws Exception {
                return new Tuple5<>(commentEvent.getReply_to_postId(),1,0,0,commentEvent.getTimestamp());
            }
        }
        ).keyBy(0)
        .window(SlidingEventTimeWindows.of(Time.hours(12), Time.minutes(30)))
        .process(new ProcessWindowFunction<Tuple5<String, Integer, Integer, Integer, Long>, Iterable<Tuple5<String, Integer, Integer, Integer, Long>>, Tuple, TimeWindow>() {

            MapState<String, Integer> replyCount;

            @Override
            public void process(Tuple tuple, Context context, Iterable<Tuple5<String, Integer, Integer, Integer, Long>> iterable, Collector<Iterable<Tuple5<String, Integer, Integer, Integer, Long>>> collector) throws Exception {
                for (Tuple5<String, Integer, Integer, Integer, Long> repEvent: iterable)
                {
                    if (replyCount.contains(repEvent.f0)) {
                        int currentCount = replyCount.get(repEvent.f0);
                        currentCount += repEvent.f1;
                        replyCount.put(repEvent.f0, currentCount);
                    }else{
                        replyCount.put(repEvent.f0, repEvent.f1);
                    }
                }

                ArrayList<Tuple5<String, Integer, Integer, Integer, Long>> output = new ArrayList<>();

                for (Tuple5<String, Integer, Integer, Integer, Long> repEvent: iterable){
                    output.add(new Tuple5<String, Integer, Integer, Integer, Long>(repEvent.f0,replyCount.get(repEvent.f0),0,0,repEvent.f4));
                }

                collector.collect(output);
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                MapStateDescriptor<String, Integer> descriptor =
                        new MapStateDescriptor<String, Integer>(
                                "count", // the state name
                                String.class, // type information
                                Integer.class);

                replyCount = getRuntimeContext().getMapState(descriptor);
            }

        });

        //Part 2: Calculation of comments for Output
        DataStream<Tuple5<String, Integer, Integer, Integer, Integer>>  commentsCounts = commentEvents
                .select("withPostID")
                .map(new MapFunction<CommentEvent, Tuple5<String, Integer, Integer, Integer, Integer>>() {
            @Override
            public Tuple5<String, Integer, Integer, Integer, Integer> map(CommentEvent commentEvent) throws Exception {
                return new Tuple5<>(commentEvent.getReply_to_postId(),0,1,0,0);
            }
        })
        .keyBy(0)
        .window(SlidingEventTimeWindows.of(Time.hours(12), Time.minutes(30)))
        .reduce(new ReduceFunction<Tuple5<String, Integer, Integer, Integer, Integer>>() {
            @Override
            public Tuple5<String, Integer, Integer, Integer, Integer> reduce(Tuple5<String, Integer, Integer, Integer, Integer> t2, Tuple5<String, Integer, Integer, Integer, Integer> t1) throws Exception {
                return new Tuple5<>(t1.f0, t1.f1 + t2.f1, t1.f2 + t2.f2, t1.f3 + t2.f3, t1.f4 + t2.f4);
            }
        });

        // Part 3: Calculation for unique user engagement for Output
        DataStream<Tuple3<String, String, Integer>> userLikesPost = likeEvents.map(new MapFunction<LikeEvent, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> map(LikeEvent likeEvent) throws Exception {
                return Tuple3.of(likeEvent.getPostId(), likeEvent.getPersonId(),1);
            }
        });

        DataStream<Tuple3<String, String, Integer>> userRepliesPost = replies.map(new MapFunction<CommentEvent, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> map(CommentEvent commentEvent) throws Exception {
                return Tuple3.of(commentEvent.getReply_to_postId(), commentEvent.getPersonId(),1);
            }
        });

        DataStream<Tuple5<String, Integer,Integer, Integer,Integer>> uniqueUserEngagementCounts = userLikesPost
                .union(userRepliesPost)
                .keyBy(0)
                .flatMap(new RichFlatMapFunction<Tuple3<String, String, Integer>, Tuple5<String, Integer, Integer, Integer, Integer>>() {

                    private transient ValueState<HashSet<String>> engagedUsers;

                    @Override
                    public void flatMap(Tuple3<String, String, Integer> stringStringIntegerTuple3, Collector<Tuple5<String, Integer, Integer, Integer, Integer>> collector) throws Exception {
                            HashSet<String> currentEngagedUsers = engagedUsers.value();

                            if (!currentEngagedUsers.contains(stringStringIntegerTuple3.f1)){
                                currentEngagedUsers.add(stringStringIntegerTuple3.f1);
                                engagedUsers.update(currentEngagedUsers);

                                collector.collect(new Tuple5<>(stringStringIntegerTuple3.f0,0,0,1,0));
                            }
                    }

                    @Override
                    public void open(Configuration config) {
                        ValueStateDescriptor<HashSet<String>>descriptor =
                                new ValueStateDescriptor<>(
                                        "engagedUsers", // the state name
                                        TypeInformation.of(new TypeHint<HashSet<String>>() {}), // type information
                                        new HashSet<String>()); // default value of the state, if nothing was set
                        engagedUsers = getRuntimeContext().getState(descriptor);
                    }

                })
                .keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.hours(12), Time.hours(1)))
                .reduce(new ReduceFunction<Tuple5<String, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public Tuple5<String, Integer, Integer, Integer, Integer> reduce(Tuple5<String, Integer, Integer, Integer, Integer> t2, Tuple5<String, Integer, Integer, Integer, Integer> t1) throws Exception {
                        return new Tuple5<>(t1.f0, t1.f1 + t2.f1, t1.f2 + t2.f2, t1.f3 + t2.f3, t1.f4 + t2.f4);
                    }
                });





        commentEvents.select("withoutPostID").map(new MapFunction<CommentEvent, String>() {
            @Override
            public String map(CommentEvent commentEvent) throws Exception {
                return commentEvent.getId();
            }
        }).print();
        env.execute("Post Kafka Consumer");
    }
}
