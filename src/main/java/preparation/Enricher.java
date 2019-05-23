package preparation;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import preparation.ReaderUtils.Topic;

import java.io.*;
import java.nio.charset.Charset;
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
            case Post:
                enrichPostEventStream(inputFile, outputFile);
                break;

            case Comment:
                enrichCommentEventStream(inputFile, outputFile);
                break;

            case Like:
                enrichLikesEventStream(inputFile, outputFile);
                break;

            case Forum_hasMember_person:
                enrichForumHasMember(inputFile, outputFile);
                break;

            case Forum_hasModerator_person:
                enrichForumHasModerator(inputFile, outputFile);
                break;

            case Person:
                enrichPerson(inputFile, outputFile);
                break;

            case Person_hasInterest_tag:
                enrichPersonHasInterestTag(inputFile, outputFile);
                break;

            case Person_speaks_language:
                enrichPersonSpeaksLanguage(inputFile, outputFile);
                break;

            case Person_knows_person:
                enrichPersonKnowsPerson(inputFile, outputFile);
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

    public void enrichForumHasMember(File input, File output) {
        Reader reader = null;
        try {
            reader = Files.newBufferedReader(input.toPath());
            CSVFormat inputFormat = CSVFormat.newFormat('|')
                    .withHeader("Forum.id", "Person.id", "joinDate")
                    .withFirstRecordAsHeader()
                    .withRecordSeparator('\n');
            CSVFormat outputFormat = CSVFormat.newFormat('|')
                    .withRecordSeparator('\n');
            CSVParser csvParser = new CSVParser(reader, inputFormat);
            BufferedWriter writer = Files.newBufferedWriter(output.toPath());
            CSVPrinter csvPrinter = new CSVPrinter(writer, outputFormat);

            csvPrinter.printRecord("Forum.id", "Person.id", "joinDate");
            for (CSVRecord record : csvParser) {
                String forumId = record.get("Forum.id");
                String personId = record.get("Person.id");
                String joinDate = record.get("joinDate");

                csvPrinter.printRecord(forumId, personId, joinDate);
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


    public void enrichForumHasModerator(File input, File output) {
        Reader reader = null;
        try {
            reader = Files.newBufferedReader(input.toPath());
            CSVFormat inputFormat = CSVFormat.newFormat('|')
                    .withHeader("Forum.id", "Person.id")
                    .withFirstRecordAsHeader()
                    .withRecordSeparator('\n');
            CSVFormat outputFormat = CSVFormat.newFormat('|')
                    .withRecordSeparator('\n');
            CSVParser csvParser = new CSVParser(reader, inputFormat);
            BufferedWriter writer = Files.newBufferedWriter(output.toPath());
            CSVPrinter csvPrinter = new CSVPrinter(writer, outputFormat);

            csvPrinter.printRecord("Forum.id", "Person.id");
            for (CSVRecord record : csvParser) {
                String forumId = record.get("Forum.id");
                String personId = record.get("Person.id");

                csvPrinter.printRecord(forumId, personId);
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

    public void enrichPerson(File input, File output) {
        Reader reader = null;
        try {
            reader = Files.newBufferedReader(input.toPath(), Charset.forName("ISO-8859-1"));
            CSVFormat inputFormat = CSVFormat.newFormat('|')
                    .withHeader("id", "firstName", "lastName", "gender", "birthday",
                            "creationDate", "locationIP", "browserUsed")
                    .withFirstRecordAsHeader()
                    .withRecordSeparator('\n');
            CSVFormat outputFormat = CSVFormat.newFormat('|')
                    .withRecordSeparator('\n');
            CSVParser csvParser = new CSVParser(reader, inputFormat);
            BufferedWriter writer = Files.newBufferedWriter(output.toPath());
            CSVPrinter csvPrinter = new CSVPrinter(writer, outputFormat);

            csvPrinter.printRecord("id", "firstName", "lastName", "gender", "birthday",
                    "creationDate", "locationIP", "browserUsed");
            for (CSVRecord record : csvParser) {
                String id = record.get("id");
                String firstName = record.get("firstName");
                String lastName = record.get("lastName");
                String gender = record.get("gender");
                String birthday = record.get("birthday");
                String creationDate = record.get("creationDate");
                String locationIP = record.get("locationIP");
                String browserUsed = record.get("browserUsed");

                csvPrinter.printRecord(id, firstName, lastName, gender, birthday, creationDate,
                        locationIP, browserUsed);
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

    public void enrichPersonHasInterestTag(File input, File output) {
        Reader reader = null;
        try {
            reader = Files.newBufferedReader(input.toPath(), Charset.forName("ISO-8859-1"));
            CSVFormat inputFormat = CSVFormat.newFormat('|')
                    .withHeader("Person.id", "Tag.id")
                    .withFirstRecordAsHeader()
                    .withRecordSeparator('\n');
            CSVFormat outputFormat = CSVFormat.newFormat('|')
                    .withRecordSeparator('\n');
            CSVParser csvParser = new CSVParser(reader, inputFormat);
            BufferedWriter writer = Files.newBufferedWriter(output.toPath());
            CSVPrinter csvPrinter = new CSVPrinter(writer, outputFormat);

            csvPrinter.printRecord("Person.id", "Tag.id");
            for (CSVRecord record : csvParser) {
                String personId = record.get("Person.id");
                String tagId = record.get("Tag.id");

                csvPrinter.printRecord(personId, tagId);
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

    public void enrichPersonSpeaksLanguage(File input, File output) {
        Reader reader = null;
        try {
            reader = Files.newBufferedReader(input.toPath(), Charset.forName("ISO-8859-1"));
            CSVFormat inputFormat = CSVFormat.newFormat('|')
                    .withHeader("Person.id", "language")
                    .withFirstRecordAsHeader()
                    .withRecordSeparator('\n');
            CSVFormat outputFormat = CSVFormat.newFormat('|')
                    .withRecordSeparator('\n');
            CSVParser csvParser = new CSVParser(reader, inputFormat);
            BufferedWriter writer = Files.newBufferedWriter(output.toPath());
            CSVPrinter csvPrinter = new CSVPrinter(writer, outputFormat);

            csvPrinter.printRecord("Person.id", "language");
            for (CSVRecord record : csvParser) {
                String personId = record.get("Person.id");
                String language = record.get("language");

                csvPrinter.printRecord(personId, language);
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

    public void enrichPersonKnowsPerson(File input, File output) {
        Reader reader = null;
        try {
            reader = Files.newBufferedReader(input.toPath(), Charset.forName("ISO-8859-1"));
            ((BufferedReader) reader).readLine();
            CSVFormat inputFormat = CSVFormat.newFormat('|')
                    .withFirstRecordAsHeader()
                    .withRecordSeparator('\n');
            CSVFormat outputFormat = CSVFormat.newFormat('|')
                    .withRecordSeparator('\n');
            CSVParser csvParser = new CSVParser(reader, inputFormat);
            BufferedWriter writer = Files.newBufferedWriter(output.toPath());
            CSVPrinter csvPrinter = new CSVPrinter(writer, outputFormat);
            csvPrinter.printRecord("Person1.id", "Person2.id");
            for (CSVRecord record : csvParser) {
                String personId1 = record.get(0);
                String personId2 = record.get(1);

                csvPrinter.printRecord(personId1, personId2);
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
