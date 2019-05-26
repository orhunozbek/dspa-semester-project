package test.Task1;

import analytical_tasks.task1.Task1;
import analytical_tasks.task1.Task1_CounterKeyedProcessFunction;
import kafka.EventKafkaProducer;
import main.Main;
import model.CommentEvent;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import preparation.ReaderUtils;
import preparation.StreamDataPreparation;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static preparation.ReaderUtils.Topic.Comment;
import static preparation.ReaderUtils.Topic.Like;

public class CounterKeyedProcessFunctionTest {

    private static Logger logger = LoggerFactory.getLogger(CounterKeyedProcessFunctionTest.class);

    /**
     * Counter data structures which are used to get count events at any point in time. The keys are postIDs, indicating
     * the posts which are associated with the counters, values are lists of timestamps, they are sorted in ascending
     * order at all times. They represents the time of the incoming events.
     */
    private HashMap<String, ArrayList<Long>> commentCounter = new HashMap<>();
    private HashMap<String, ArrayList<Long>> replyCounter = new HashMap<>();
    private HashMap<String, ArrayList<Long>> userEngagementCounter = new HashMap<>();

    /**
     * A map which maps commentIDs to postIDs. It is used to
     */
    private HashMap<String, String> postMap = new HashMap<>();

    // A map which projects posts to a set of users who are engaged by that post
    private HashMap<String, HashSet<String>> currentEngagementMap = new HashMap<>();

    private ImmutableMap<String, HashMap<String, ArrayList<Long>>> counterMap = ImmutableMap.of(
            "userEngagement", userEngagementCounter,
            "comment", commentCounter,
            "reply", replyCounter
    );

    private static final String configurationFilePath = "src/main/java/test/Task1/testConfig.properties";
    private static final String kafkaBrokerList = "localhost:9092";

    private Configuration testSetup() {
        Main.setGlobalConfig(configurationFilePath);
        return Main.getGlobalConfig();
    }

    private void addElement(HashMap<String, ArrayList<Long>> counter, String postId, Long timestamp) {

        ArrayList<Long> values;

        if (counter.containsKey(postId)) values = counter.get(postId);
        else values = new ArrayList<>();

        values.add(timestamp);
        counter.put(postId, values);
    }

    private int getCountAtTime(long timestamp, String postID, String counterType) {
        HashMap<String, ArrayList<Long>> counter = counterMap.get(counterType);
        ArrayList<Long> timestamps = counter.get(postID);
        int count = 0;

        for (long t : timestamps) {
            if (t > timestamp) break;
            count++;
        }

        return count;
    }

    private Tuple2<Long, Iterable<Tuple3<String, String, Integer>>> parseKafkaOutput(String s) {

        long timestamp = 0L;
        ArrayList<Tuple3<String, String, Integer>> resultArray = new ArrayList<>();

        // Create a Pattern object
        Pattern timeStampPattern = Pattern.compile("\\d+");
        Pattern outputTuplePattern = Pattern.compile("\\d+,\\w+,\\d+");

        // Now create matcher object.
        Matcher tm = timeStampPattern.matcher(s);
        if (tm.find()) timestamp = Long.parseLong(tm.group(0));

        Matcher om = outputTuplePattern.matcher(s);
        while (om.find()) {
            String[] tuple = om.group(0).split(",");
            resultArray.add(new Tuple3<>(tuple[0], tuple[1], Integer.parseInt(tuple[2])));
        }

        assert timestamp != 0L && resultArray.size() != 0;
        return new Tuple2<>(timestamp, resultArray);
    }

    private long getUpdateTimeForEvent(String s) {
        switch (s) {
            case "comment":
                return Task1_CounterKeyedProcessFunction.replyUpdateTime;
            case "reply":
                return Task1_CounterKeyedProcessFunction.replyUpdateTime;
            case "userEngagement":
                return Task1_CounterKeyedProcessFunction.userEngagementUpdateTime;
            default:
                return 0L;
        }
    }

    private void constructPostMap(Configuration config) throws IOException {
        config.getString("workingDirectory");
        File eventStreamFile = ReaderUtils.getFile(ReaderUtils.Directory.WorkingDirectory, Comment);
        assert eventStreamFile != null;

        Reader reader = Files.newBufferedReader(eventStreamFile.toPath());

        CSVFormat inputFormat = CSVFormat.newFormat('|')
                .withHeader(ReaderUtils.getHeaderFor(ReaderUtils.Directory.WorkingDirectory, Comment))
                .withRecordSeparator('\n');

        CSVParser csvParser = new CSVParser(reader, inputFormat);

        for (CSVRecord record : csvParser) {
            String id = record.get("id");
            String commentId = record.get("reply_to_commentId");
            String postId = record.get("reply_to_postId");

            if (!postId.equals("")) postMap.put(id, postId);
            else {
                postId = postMap.get(commentId);
                postMap.put(id, postId);
            }
        }
    }


    private void constructCommentAndReplyCounter(Configuration config) throws IOException {
        config.getString("workingDirectory");
        File eventStreamFile = ReaderUtils.getFile(ReaderUtils.Directory.WorkingDirectory, Comment);
        assert eventStreamFile != null;

        Reader reader = Files.newBufferedReader(eventStreamFile.toPath());

        CSVFormat inputFormat = CSVFormat.newFormat('|')
                .withHeader(ReaderUtils.getHeaderFor(ReaderUtils.Directory.WorkingDirectory, Comment))
                .withRecordSeparator('\n');

        CSVParser csvParser = new CSVParser(reader, inputFormat);

        for (CSVRecord record : csvParser) {
            String id = record.get("id");
            String postID = postMap.get(id);
            Long timestamp = Long.parseLong(record.get("timeMilisecond"));

            assert record.get("reply_to_postId").equals("") || postID.equals(record.get("reply_to_postId"));

            if (record.get("reply_to_postId").equals("")) {
                addElement(replyCounter, postID, timestamp);
            } else {
                addElement(replyCounter, postID, timestamp);
                addElement(commentCounter, postID, timestamp);
            }
        }

    }

    /**
     *  Helper function which constructs userEngagementMap and userEngagementCounter
     */
    private void constructUserEngagementCounter(Configuration config) throws IOException {
        config.getString("workingDirectory");
        ReaderUtils.Topic[] topics = new ReaderUtils.Topic[]{Like, Comment};

        // PostID, PersonID, Timestamp
        ArrayList<Tuple3<String, String, Long>> combinedEvents = new ArrayList<>();

        for (ReaderUtils.Topic topic : topics) {
            File eventStreamFile = ReaderUtils.getFile(ReaderUtils.Directory.WorkingDirectory, topic);
            assert eventStreamFile != null;

            Reader reader = Files.newBufferedReader(eventStreamFile.toPath());

            CSVFormat inputFormat = CSVFormat.newFormat('|')
                    .withHeader(ReaderUtils.getHeaderFor(ReaderUtils.Directory.WorkingDirectory, topic))
                    .withRecordSeparator('\n');

            CSVParser csvParser = new CSVParser(reader, inputFormat);

            // Populate combined events
            for (CSVRecord record : csvParser) {

                if (topic.equals(Comment)) {

                    String id = record.get("id");
                    String postID = postMap.get(id);
                    String personID = record.get("personId");
                    Long timestamp = Long.parseLong(record.get("timeMilisecond"));

                    assert record.get("reply_to_postId").equals("") || postID.equals(record.get("reply_to_postId"));

                    combinedEvents.add(new Tuple3<>(postID, personID, timestamp));

                } else {
                    // Event is a like
                    String personID = record.get("Person.id");
                    String postID = record.get("Post.id");
                    Long timestamp = Long.parseLong(record.get("timeMilisecond"));
                    combinedEvents.add(new Tuple3<>(postID, personID, timestamp));
                }

            }
        }

        // Sort combined events by their timestamps to create a stream of
        // userEngagement events
        combinedEvents.sort(Comparator.comparingLong(o -> o.f2));

        for (Tuple3<String, String, Long> event : combinedEvents) {
            if (currentEngagementMap.containsKey(event.f0)) {
                if (!currentEngagementMap.get(event.f0).contains(event.f1)) {
                    HashSet<String> engagedUsers = currentEngagementMap.get(event.f0);
                    engagedUsers.add(event.f1);
                    currentEngagementMap.put(event.f0, engagedUsers);
                    addElement(userEngagementCounter, event.f0, event.f2);
                }
            } else {
                HashSet<String> engagedUsers = new HashSet<>();
                engagedUsers.add(event.f1);
                currentEngagementMap.put(event.f0, engagedUsers);
                addElement(userEngagementCounter, event.f0, event.f2);
            }
        }


    }


    @Test
    public void testCommentMatching() {

        // Create a new working directory and put input data. Working directory will be completely ordered and
        // respect to integrity constraints
        Configuration configuration = testSetup();
        StreamDataPreparation streamDataPreparation = new StreamDataPreparation();
        assert (streamDataPreparation.start());

        // Clean all topics
        Properties props = new Properties();
        props.put("delete.topic.enable", "true");
        props.setProperty("bootstrap.servers", kafkaBrokerList);

        AdminClient client = KafkaAdminClient.create(props);
        String [] topicsToBeDeleted = {"comments", "likes", "activityCounter-test"};
        client.deleteTopics(Arrays.asList(topicsToBeDeleted));

        String[] args = {configurationFilePath, "activityCounter-test"};

        // Create answer keys and also stream the data to Kafka
        try {
            constructPostMap(configuration);
            constructCommentAndReplyCounter(configuration);
            constructUserEngagementCounter(configuration);

            EventKafkaProducer.streamToKafka(Comment);
            EventKafkaProducer.streamToKafka(Like);

            new Thread(() -> {
                try {
                    Task1.main(args);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();

        } catch (Exception e) {
            e.printStackTrace();
        }

        // Create a Kafka consumer to stream the answers from
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", kafkaBrokerList);
        kafkaProps.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaProps.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        // Setup a unique Consumer name
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "Consumer" + System.currentTimeMillis());


        final Consumer<Long, String> consumer = new KafkaConsumer<>(kafkaProps);
        consumer.subscribe(Collections.singletonList("activityCounter-test"));

        final int giveUp = 1000;
        int noRecordsCount = 0;
        int count = 0;

        while (true) {
            final ConsumerRecords<Long, String> consumerRecords =
                    consumer.poll(1000);
            if (consumerRecords.count() == 0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }

            for (ConsumerRecord<Long, String> record : consumerRecords) {
                count++;

                Tuple2<Long, Iterable<Tuple3<String, String, Integer>>> parsedRecord = parseKafkaOutput(record.value());

                for (Tuple3<String, String, Integer> countInfo : parsedRecord.f1) {
                    if (countInfo.f2 != 0) {

                        int realCountInfo = getCountAtTime(parsedRecord.f0, countInfo.f0, countInfo.f1);
                        int delayedCountInfo = getCountAtTime(
                                parsedRecord.f0 - getUpdateTimeForEvent(countInfo.f1) - 1000,
                                countInfo.f0,
                                countInfo.f1);

                        if (!(countInfo.f2 >= delayedCountInfo && countInfo.f2 <= realCountInfo)) {
                            logger.warn("Count not expected, stream output: {}, minimum expected count {}, original count {}",
                                    countInfo.toString(), delayedCountInfo, realCountInfo);
                        }
                    }
                }

                if (count % 100 == 0) logger.info(String.format("Processed %d windows, window starting time: %d"
                        , count, parsedRecord.f0));
            }

            consumer.commitAsync();
        }

        logger.info("Test complete!");
        consumer.close();


    }
}
