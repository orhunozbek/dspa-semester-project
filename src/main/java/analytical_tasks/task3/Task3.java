package analytical_tasks.task3;

import kafka.EventDeserializer;
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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static analytical_tasks.task3.Task3_Metrics.*;
import static main.Main.setGlobalConfig;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;

public class Task3 {

    // Initialize Logger
    private static Logger logger = LoggerFactory.getLogger(Task3.class);

    private static final String DEFAULT_CONFIG_LOCATION = "config.properties";
    private static final String kafkaBrokerList = "localhost:9092";


    public static void main(String[] args) throws Exception {

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
        DataStream<CommentEvent> commentEventsSource = env
                .addSource(new FlinkKafkaConsumer011<>("comments", new EventDeserializer<>(CommentEvent.class), kafkaProps))
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<CommentEvent>(Time.seconds(maxDelay)) {
                            @Override
                            public long extractTimestamp(CommentEvent element) {
                                return element.getTimeMilisecond(); }
                });

        // DataStream for likes
        // Going to be used for user engagement and active post tracking
        DataStream<LikeEvent> likeEventsSource = env
                .addSource(new FlinkKafkaConsumer011<>("likes", new EventDeserializer<>(LikeEvent.class), kafkaProps))
                .assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<LikeEvent>(Time.seconds(maxDelay)) {
                    @Override
                    public long extractTimestamp(LikeEvent element) {
                        return element.getTimeMilisecond(); }
                });

        // DataStream for likes
        // Going to be used for user engagement and active post tracking
        DataStream<PostEvent> postsEventSource = env
                .addSource( new FlinkKafkaConsumer011<>("posts", new EventDeserializer<>(PostEvent.class), kafkaProps))
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<PostEvent>(Time.seconds(maxDelay)) {
                @Override
                public long extractTimestamp(PostEvent element) {
                    return element.getTimeMilisecond();
                }});


        // (PersonId, FeatureID, FeatureValue, Time)
        DataStream<Tuple4<String, Integer, Double, Long>> postFeatures = postsEventSource
                .keyBy((KeySelector<PostEvent, String>) PostEvent::getPersonId)
                .process(new KeyedProcessFunction<String, PostEvent, Tuple4<String, Integer, Double, Long>>() {

                    private transient ValueState<Long> processedWatermark;
                    private transient ValueState<Double> calculateUniqueWordsOverWordsMetricSum;
                    private transient ValueState<Double> profanityFilteredWords;
                    private transient ValueState<Integer> count;

                    @Override
                    public void processElement(PostEvent postEvent, Context context,
                                               Collector<Tuple4<String, Integer, Double, Long>> collector) throws Exception {

                        String post = postEvent.getContent();

                        Double uniqueOverWord = calculateUniqueWordsOverWordsMetric(post);
                        calculateUniqueWordsOverWordsMetricSum.update(
                                calculateUniqueWordsOverWordsMetricSum.value() +
                                        uniqueOverWord);

                        // Increment the number of total posts
                        count.update(count.value() + 1);

                        // Profanity filtering
                        profanityFilteredWords.update(profanityFilteredWords.value() + countBadWords(post));


                        if (processedWatermark.value() < context.timerService().currentWatermark()) {

                            processedWatermark.update(context.timerService().currentWatermark());

                            collector.collect(new Tuple4<>(postEvent.getPersonId(),
                                    0,
                                    calculateUniqueWordsOverWordsMetricSum.value()/count.value(),
                                    postEvent.getTimeMilisecond()));

                            collector.collect(new Tuple4<>(postEvent.getPersonId(),
                                    5,
                                    profanityFilteredWords.value()/count.value(),
                                    postEvent.getTimeMilisecond()));

                        }

                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);

                        // Feature, number of unique words over words in a Comment, Text feature
                        ValueStateDescriptor<Double> calculateUniqueWordsOverWordsMetricSumDescriptor =
                                new ValueStateDescriptor<>("calculateUniqueWordsOverWordsMetricSum",
                                        BasicTypeInfo.DOUBLE_TYPE_INFO, 0.0);

                        calculateUniqueWordsOverWordsMetricSum =
                                getRuntimeContext().getState(calculateUniqueWordsOverWordsMetricSumDescriptor);

                        // Number of messages which did not pass the profanity filter
                        ValueStateDescriptor<Double> profanityFilteredWordsDescriptor =
                                new ValueStateDescriptor<>("profanityFilteredMessages",
                                        BasicTypeInfo.DOUBLE_TYPE_INFO, 0.0);

                        profanityFilteredWords =
                                getRuntimeContext().getState(profanityFilteredWordsDescriptor);

                        // State for counter of CommentEvents for each PersonID
                        ValueStateDescriptor<Integer> countDescriptor =
                                new ValueStateDescriptor<>("count",
                                        BasicTypeInfo.INT_TYPE_INFO, 0);

                        count = getRuntimeContext().getState(countDescriptor);

                        ValueStateDescriptor<Long> processedWatermarkDescriptor =
                                new ValueStateDescriptor<>("processedWatermark",
                                        BasicTypeInfo.LONG_TYPE_INFO, 0L);

                        processedWatermark = getRuntimeContext().getState(processedWatermarkDescriptor);

                    }
                });

        // (PersonId, FeatureID, FeatureValue, Time)
        DataStream<Tuple4<String, Integer, Double, Long>> commentFeatures = commentEventsSource
                .keyBy((KeySelector<CommentEvent, String>) CommentEvent::getPersonId)
                .process(new KeyedProcessFunction<String, CommentEvent, Tuple4<String, Integer, Double, Long>>() {

                    private transient ValueState<Double> calculateUniqueWordsOverWordsMetricSum;
                    private transient ValueState<Double> profanityFilteredWords;
                    private transient MapState<String, Integer> numberOfWordsUsed;
                    private transient ValueState<Integer> count;

                    private transient ValueState<Long> processedWatermark;

                    @Override
                    public void processElement(CommentEvent commentEvent, Context context,
                                               Collector<Tuple4<String, Integer, Double, Long>> collector) throws Exception {

                        String comment = commentEvent.getContent();

                        Double uniqueOverWord = calculateUniqueWordsOverWordsMetric(comment);
                        calculateUniqueWordsOverWordsMetricSum.update(
                                calculateUniqueWordsOverWordsMetricSum.value() +
                                        uniqueOverWord);

                        // Count distinct words in the comment
                        numberOfWordsUsed.putAll(vectorize(comment));

                        // Increment the number of total posts
                        count.update(count.value() + 1);

                        // Profanity filtering
                        profanityFilteredWords.update(profanityFilteredWords.value() + countBadWords(comment));

                        if (processedWatermark.value() < context.timerService().currentWatermark()) {

                            processedWatermark.update(context.timerService().currentWatermark());

                            collector.collect(new Tuple4<>(commentEvent.getPersonId(),
                                    1,
                                    calculateUniqueWordsOverWordsMetricSum.value()/count.value(),
                                    commentEvent.getTimeMilisecond()));

                            collector.collect(new Tuple4<>(commentEvent.getPersonId(),
                                    2,
                                    profanityFilteredWords.value()/count.value(),
                                    commentEvent.getTimeMilisecond()));

                            double distinctWords = 0.0;
                            for (String key : numberOfWordsUsed.keys()){
                                distinctWords ++;
                            }

                            collector.collect(new Tuple4<>(commentEvent.getPersonId(),
                                    3,
                                    distinctWords/count.value(),
                                    commentEvent.getTimeMilisecond()));
                        }
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);

                        // All words used by a person and their counts
                        MapStateDescriptor<String, Integer> numberOfWordsUsedDescriptor =
                                new MapStateDescriptor<>(
                                        "numberOfWordsUsed",
                                        BasicTypeInfo.STRING_TYPE_INFO,
                                        BasicTypeInfo.INT_TYPE_INFO);

                        numberOfWordsUsed =  getRuntimeContext().getMapState(numberOfWordsUsedDescriptor);

                        // Feature, number of unique words over words in a Comment, Text feature
                        ValueStateDescriptor<Double> calculateUniqueWordsOverWordsMetricSumDescriptor =
                                new ValueStateDescriptor<>("calculateUniqueWordsOverWordsMetricSum",
                                        BasicTypeInfo.DOUBLE_TYPE_INFO, 0.0);

                        calculateUniqueWordsOverWordsMetricSum =
                                getRuntimeContext().getState(calculateUniqueWordsOverWordsMetricSumDescriptor);

                        // Number of messages which did not pass the profanity filter
                        ValueStateDescriptor<Double> profanityFilteredWordsDescriptor =
                                new ValueStateDescriptor<>("profanityFilteredMessages",
                                        BasicTypeInfo.DOUBLE_TYPE_INFO, 0.0);

                        profanityFilteredWords =
                                getRuntimeContext().getState(profanityFilteredWordsDescriptor);

                        // State for counter of CommentEvents for each PersonID
                        ValueStateDescriptor<Integer> countDescriptor =
                                new ValueStateDescriptor<>("count",
                                        BasicTypeInfo.INT_TYPE_INFO, 0);

                        count = getRuntimeContext().getState(countDescriptor);

                        ValueStateDescriptor<Long> processedWatermarkDescriptor =
                                new ValueStateDescriptor<>("processedWatermark",
                                        BasicTypeInfo.LONG_TYPE_INFO, 0L);

                        processedWatermark = getRuntimeContext().getState(processedWatermarkDescriptor);
                    }
                });

        MapStateDescriptor<String, PostEvent> postBroadcastStateDescriptor = new MapStateDescriptor<>(
                "PostsBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<PostEvent>() {}));

        BroadcastStream<PostEvent> postsBroadcastStream = postsEventSource
                .broadcast(postBroadcastStateDescriptor);

        DataStream<Tuple4<String, Integer, Double, Long>> likeEventStat =

                likeEventsSource.keyBy((KeySelector<LikeEvent, String>) LikeEvent::getPersonId)
                    .connect(postsBroadcastStream)
                    .process(new KeyedBroadcastProcessFunction<String, LikeEvent, PostEvent, Tuple4<String, Integer, Double, Long>>() {

                        private transient HashMap<String, String> postBelongsTo;

                        private transient ValueState<Integer> count;
                        private transient ListState<LikeEvent> likeEventsBuffer;
                        private transient ValueState<Long> processedWatermark;
                        private transient ValueState<HashSet<String>> personLikesPerson;

                        @Override
                        public void processElement(LikeEvent likeEvent, ReadOnlyContext readOnlyContext,
                                                   Collector<Tuple4<String, Integer, Double, Long>> collector) throws Exception {

                            count.update(count.value()+1);
                            likeEventsBuffer.add(likeEvent);

                            if (processedWatermark.value() < readOnlyContext.timerService().currentWatermark()) {

                                processedWatermark.update(readOnlyContext.timerService().currentWatermark());
                                ArrayList<LikeEvent> notToBeProcessedItems = new ArrayList<>();
                                HashSet<String> currentPersonLikesPerson = personLikesPerson.value();

                                for (LikeEvent event : likeEventsBuffer.get()){

                                    if (event.getTimeMilisecond() < processedWatermark.value()){

                                        if (postBelongsTo.containsKey(event.getPostId())){

                                            String personID = postBelongsTo.get(event.getPostId());
                                            currentPersonLikesPerson.add(personID);

                                        }else{
                                            logger.warn("LikeEvent with postID:{} could not be matched.", event.getPostId());
                                            notToBeProcessedItems.add(event);
                                        }

                                    }
                                    else {
                                        notToBeProcessedItems.add(event);
                                    }
                                }

                                personLikesPerson.update(currentPersonLikesPerson);
                                likeEventsBuffer.clear();
                                likeEventsBuffer.addAll(notToBeProcessedItems);

                                collector.collect(new Tuple4<>(likeEvent.getPersonId(),
                                        4,
                                        (double) currentPersonLikesPerson.size() / count.value()
                                        ,likeEvent.getTimestamp()));
                            }

                        }

                        @Override
                        public void processBroadcastElement(PostEvent postEvent, Context context,
                                                            Collector<Tuple4<String, Integer, Double, Long>> collector) throws Exception {
                             postBelongsTo.put(postEvent.getId(), postEvent.getPersonId());
                        }

                        @Override
                        public void open(Configuration parameters) throws Exception {
                            super.open(parameters);

                            // All words used by a person and their counts
                            postBelongsTo = new HashMap<>();

                            ValueStateDescriptor<Integer> countDescriptor =
                                    new ValueStateDescriptor<>("count",
                                            BasicTypeInfo.INT_TYPE_INFO, 0);

                            count = getRuntimeContext().getState(countDescriptor);


                            ListStateDescriptor<LikeEvent> likeEventsBufferDescriptor = new ListStateDescriptor<LikeEvent>(
                                    "likeEventsBuffer",
                                    TypeInformation.of(LikeEvent.class)
                            );

                            likeEventsBuffer = getRuntimeContext().getListState(likeEventsBufferDescriptor);


                            ValueStateDescriptor<Long> processedWatermarkDescriptor =
                                    new ValueStateDescriptor<>("processedWatermark",
                                            BasicTypeInfo.LONG_TYPE_INFO, 0L);

                            processedWatermark = getRuntimeContext().getState(processedWatermarkDescriptor);

                            ValueStateDescriptor<HashSet<String>> personLikesPersonDescriptor =
                                    new ValueStateDescriptor<>(
                                            "personLikesPerson",
                                            TypeInformation.of(new TypeHint<HashSet<String>>() {}),
                                            new HashSet<>());

                            personLikesPerson = getRuntimeContext().getState(personLikesPersonDescriptor);

                        }
                    });

        // (PersonID, featureID, FeatureValue (User), FeatureValue (Current Mean))
        DataStream<Tuple5<String, Integer, Double, Double, Long>> outlierDetection = postFeatures
                .union(commentFeatures)
                .union(likeEventStat)
                // Partition data by features
                .keyBy(1)
                .process(new Task3_OutlierDetectionProcess());


        outlierDetection.print();
        env.execute("Post Kafka Consumer");


    }

}
