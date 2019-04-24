package analytical_tasks.task1;

import kafka.EventDeserializer;
import model.CommentEvent;
import model.LikeEvent;
import org.apache.commons.collections.map.LRUMap;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
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
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;

public class Task1 {

    public static void main(String[] args) throws Exception {
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
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<CommentEvent>(Time.seconds(10)) {

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
                                //final MapState<String, String>  state = getRuntimeContext().getMapState(mapStateDesc);
                                mapState.put(commentEvent.getId(),commentEvent.getReply_to_postId());
                                collector.collect(commentEvent);

                                Object arr = cache.get(commentEvent.getReply_to_commentId());

                                if(arr!= null){
                                    for (CommentEvent event: (ArrayList<CommentEvent>)arr) {

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

                                        collector.collect(updatedCommentEvent);

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

                                    collector.collect(updatedCommentEvent);

                                }else{

                                    if(cache.get(commentEvent.getReply_to_commentId())== null){
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




        // To get a list of active posts, map all replies
        DataStream<Tuple2<String, Integer>> activePosts =
                likeEvents.union(replies.map(new MapFunction<CommentEvent, Tuple2<String,
                        Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(CommentEvent event) {
                        return new Tuple2<>(
                                event.getReply_to_postId(),1);
                    }

                }))
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
