package preparation;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import preparation.ReaderUtils.Topic;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.TimeZone;


public class Enricher {

    // Initialize Logger
    private static Logger logger = LoggerFactory.getLogger(Enricher.class);

    HashMap<String, Long> postEvents = new HashMap<>();
    HashMap<String, Long> commentEvents = new HashMap<>();

    public void enrichStream(Topic topic, File inputFile, File outputFile) {

        switch (topic) {
            case Comment:
                enrichCommentEventStream(inputFile, outputFile);
                break;

            case Like:
                enrichLikesEventStream(inputFile, outputFile);
                break;

            case Post:
                enrichPostEventStream(inputFile, outputFile);
                break;
        }
    }

    public long convertToEpoch(String time, int formatId) {
        DateFormat sdf;
        if (formatId == 0) {
            sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        } else {
            sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        }
        Date date = null;
        try {
            date = sdf.parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date.getTime();
    }

    public void enrichCommentEventStream(File input, File output) {
        Reader reader = null;
        try {
            reader = Files.newBufferedReader(input.toPath());
            CSVFormat inputFormat = CSVFormat.newFormat('|')
                    .withHeader("id", "personId", "creationDate", "locationIP",
                            "browserUsed", "content", "reply_to_postId", "reply_to_commentId", "placeId")
                    .withFirstRecordAsHeader()
                    .withRecordSeparator('\n');
            CSVFormat outputFormat = CSVFormat.newFormat('|')
                    .withRecordSeparator('\n');
            CSVParser csvParser = new CSVParser(reader, inputFormat);
            BufferedWriter writer = Files.newBufferedWriter(output.toPath());
            CSVPrinter csvPrinter = new CSVPrinter(writer, outputFormat);
            long currentTimestamp = 0L;

            for (CSVRecord record : csvParser) {
                String id = record.get("id");
                String personId = record.get("personId");
                String creationDate = record.get("creationDate");
                String locationIP = record.get("locationIP");
                String browserUsed = record.get("browserUsed");
                String content = record.get("content");
                String replyToPostId = record.get("reply_to_postId");
                String replyToCommentId = record.get("reply_to_commentId");
                String placeId = record.get("placeId");
                long epochNumber = convertToEpoch(creationDate, 0);

                if (epochNumber >= currentTimestamp) {
                    if (replyToPostId.equals("")) {
                        if (commentEvents.containsKey(replyToCommentId)) {
                            commentEvents.put(id, epochNumber);

                            csvPrinter.printRecord(epochNumber,
                                    id,
                                    personId,
                                    creationDate,
                                    locationIP,
                                    browserUsed,
                                    content,
                                    replyToPostId,
                                    replyToCommentId,
                                    placeId);
                        } else {
                            logger.warn("CommentID: {} does not exists. Ignoring Comment with ID: {}"
                                    , replyToCommentId, id);
                        }

                    } else {
                        if (postEvents.containsKey(replyToPostId) && postEvents.get(replyToPostId) < epochNumber) {

                            commentEvents.put(id, epochNumber);

                            csvPrinter.printRecord(epochNumber,
                                    id,
                                    personId,
                                    creationDate,
                                    locationIP,
                                    browserUsed,
                                    content,
                                    replyToPostId,
                                    replyToCommentId,
                                    placeId);

                        } else if (!postEvents.containsKey(replyToPostId)) {
                            logger.warn("PostID: {} does not exists. Ignoring Comment with ID: {}", replyToPostId, id);
                        } else {
                            logger.warn("Post with timestamp {} comes after Comment timestamp {}. " +
                                            "Ignoring Comment with ID {}, replying to postID: {}.",
                                    postEvents.get(replyToPostId), epochNumber, id, replyToPostId);
                        }
                    }

                } else {
                    logger.warn("Sample with a timestamp that breaks monotonicity. " +
                            "Ignoring Comment with ID: {}, Event Timestamp: {}, Current Timestamp: {}", id, epochNumber, currentTimestamp);
                }


            }

            csvPrinter.flush();

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void enrichLikesEventStream(File input, File output) {
        Reader reader = null;
        try {
            reader = Files.newBufferedReader(input.toPath());
            CSVFormat inputFormat = CSVFormat.newFormat('|')
                    .withHeader("Person.id", "Post.id", "creationDate")
                    .withFirstRecordAsHeader()
                    .withRecordSeparator('\n');
            CSVFormat outputFormat = CSVFormat.newFormat('|')
                    .withRecordSeparator('\n');
            CSVParser csvParser = new CSVParser(reader, inputFormat);
            BufferedWriter writer = Files.newBufferedWriter(output.toPath());
            CSVPrinter csvPrinter = new CSVPrinter(writer, outputFormat);
            long currentTimestamp = 0L;

            for (CSVRecord record : csvParser) {
                String personId = record.get("Person.id");
                String postId = record.get("Post.id");
                String creationDate = record.get("creationDate");
                long epochNumber = convertToEpoch(creationDate, 1);

                if (epochNumber >= currentTimestamp) {
                    if (postEvents.containsKey(postId) && postEvents.get(postId) < epochNumber) {
                        currentTimestamp = epochNumber;
                        csvPrinter.printRecord(epochNumber, personId, postId, creationDate);

                    } else if (!postEvents.containsKey(postId)) {
                        logger.warn("PostID: {} does not exists. Ignoring Like", postId);
                    } else {
                        logger.warn("PostID with timestamp {} comes after Like timestamp {}. " +
                                        "Ignoring Like with PostID: {}, PersonID {}.",
                                postEvents.get(postId), epochNumber, postId, personId);
                    }
                } else {
                    logger.warn("Sample with a timestamp that breaks monotonicity. " +
                                    "Ignoring Like with PostID: {}, PersonID {}, Event Timestamp {}, Current Timestamp: {}."
                            , postId, personId, epochNumber, currentTimestamp);
                }

            }

            csvPrinter.flush();

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void enrichPostEventStream(File input, File output) {
        Reader reader = null;
        try {
            reader = Files.newBufferedReader(input.toPath());
            CSVFormat inputFormat = CSVFormat.newFormat('|')
                    .withHeader("id", "personId", "creationDate", "imageFile", "locationIP", "browserUsed", "language",
                            "content", "tags", "forumId", "placeId")
                    .withFirstRecordAsHeader()
                    .withRecordSeparator('\n');
            CSVFormat outputFormat = CSVFormat.newFormat('|')
                    .withRecordSeparator('\n');
            CSVParser csvParser = new CSVParser(reader, inputFormat);
            BufferedWriter writer = Files.newBufferedWriter(output.toPath());
            CSVPrinter csvPrinter = new CSVPrinter(writer, outputFormat);
            long currentTimestamp = 0L;

            for (CSVRecord record : csvParser) {
                String id = record.get("id");
                String personId = record.get("personId");
                String creationDate = record.get("creationDate");
                String imageFile = record.get("imageFile");
                String locationIP = record.get("locationIP");
                String browserUsed = record.get("browserUsed");
                String language = record.get("language");
                String content = record.get("content");
                String tags = record.get("tags");
                String forumId = record.get("forumId");
                String placeId = record.get("placeId");
                long epochNumber = convertToEpoch(creationDate, 0);

                if (epochNumber >= currentTimestamp) {
                    currentTimestamp = epochNumber;
                    postEvents.put(id, epochNumber);

                    csvPrinter.printRecord(epochNumber,
                            id,
                            personId,
                            creationDate,
                            imageFile,
                            locationIP,
                            browserUsed,
                            language,
                            content,
                            tags,
                            forumId,
                            placeId);

                } else {
                    logger.warn("Sample with a timestamp that breaks monotonicity. " +
                            "Ignoring Post with ID: {}, Event Timestamp: {}, Current Timestamp: {}", id, epochNumber, currentTimestamp);
                }

            }

            csvPrinter.flush();

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
