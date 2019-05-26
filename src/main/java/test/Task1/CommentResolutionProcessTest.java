package test.Task1;

import analytical_tasks.task1.Task1_CommentResolutionProcess;
import kafka.EventDeserializer;
import kafka.EventKafkaProducer;
import main.Main;
import model.CommentEvent;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import preparation.ReaderUtils;
import preparation.ReorderProcess;
import preparation.StreamDataPreparation;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.util.*;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static preparation.ReaderUtils.Topic.Comment;

public class CommentResolutionProcessTest {

    private static Logger logger = LoggerFactory.getLogger(CommentResolutionProcessTest.class);


    private HashMap<String,String> postMap =  new HashMap<>();
    private static final String configurationFilePath = "src/main/java/test/Task1/testConfig.properties";
    private static final String kafkaBrokerList = "localhost:9092";

    private Configuration testSetup() {
        Main.setGlobalConfig(configurationFilePath);
        return Main.getGlobalConfig();
    }


    private void constructPostMap(Configuration config) throws IOException {
        config.getString("workingDirectory");
        File eventStreamFile = ReaderUtils.getFile(ReaderUtils.Directory.WorkingDirectory, ReaderUtils.Topic.Comment);
        assert eventStreamFile != null;

        Reader reader = Files.newBufferedReader(eventStreamFile.toPath());

        CSVFormat inputFormat = CSVFormat.newFormat('|')
                .withHeader(ReaderUtils.getHeaderFor(ReaderUtils.Directory.WorkingDirectory, ReaderUtils.Topic.Comment))
                .withRecordSeparator('\n');

        CSVParser csvParser = new CSVParser(reader, inputFormat);

        for(CSVRecord record : csvParser) {
            String id = record.get("id");
            String commentId = record.get("reply_to_commentId");
            String postId = record.get("reply_to_postId");

            if (!postId.equals("")) postMap.put(id,postId);
            else{
                postId = postMap.get(commentId);
                postMap.put(id,postId);
            }
        }

    }

    private void startCommentMatchingProcess(Configuration configuration){
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", kafkaBrokerList);
        kafkaProps.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaProps.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");

        // Source for all comment events
        DataStream<CommentEvent> commentEventsSource = env.addSource(
                new FlinkKafkaConsumer011<>("comments", new EventDeserializer<>(CommentEvent.class), kafkaProps));

        int maxDelay = configuration.getInt("maxDelayInSec");

        SplitStream<CommentEvent> commentEvents = commentEventsSource
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
                .process(new Task1_CommentResolutionProcess());

        FlinkKafkaProducer011<String> prod =
                new FlinkKafkaProducer011<>(
                        kafkaBrokerList, // broker list
                        "commentMatching-test",
                        new SimpleStringSchema());

        replies.map(new MapFunction<CommentEvent, String>() {
            @Override
            public String map(CommentEvent commentEvent) throws Exception {
                return commentEvent.getId()+ "," + commentEvent.getReply_to_postId();
            }
        }).addSink(prod);

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCommentMatching() {
        Configuration configuration = testSetup();
        StreamDataPreparation streamDataPreparation = new StreamDataPreparation();
        assert(streamDataPreparation.start());

        // Clean all topics
        Properties props = new Properties();
        props.put("delete.topic.enable", "true");
        props.setProperty("bootstrap.servers", kafkaBrokerList);

        AdminClient client = KafkaAdminClient.create(props);
        String [] topicsToBeDeleted = {"comments", "commentMatching-test"};
        client.deleteTopics(Arrays.asList(topicsToBeDeleted));

        try {
            constructPostMap(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }

        EventKafkaProducer.streamToKafka(Comment);
        new Thread(() -> startCommentMatchingProcess(configuration)).start();


        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", kafkaBrokerList);
        kafkaProps.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaProps.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG,
                "Consumer" +
                        System.currentTimeMillis());


        final Consumer<Long, String> consumer =  new KafkaConsumer<>(kafkaProps);
        consumer.subscribe(Collections.singletonList("commentMatching-test"));

        final int giveUp = 1000;
        int noRecordsCount = 0;

        while (true) {
            final ConsumerRecords<Long, String> consumerRecords =
                    consumer.poll(1000);
            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }

            for (ConsumerRecord<Long,String> record: consumerRecords){
                String [] vals = record.value().split(",");
                System.out.println(String.format("Replies left %d", postMap.size()));
                assert(postMap.get(vals[0]).equals(vals[1]));
                postMap.remove(vals[0]);
            }

            consumer.commitAsync();
        }
        consumer.close();
        logger.info("Test complete!");

    }

}
