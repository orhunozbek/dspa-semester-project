package preparation;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Enricher {
    public long convertToEpoch(String time, int formatId) {
        DateFormat sdf;
        if(formatId == 0) {
            sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        } else {
            sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
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

            long time = 0;

            for(CSVRecord record : csvParser) {
                String id = record.get("id");
                String personId = record.get("personId");
                String creationDate = record.get("creationDate");
                String locationIP = record.get("locationIP");
                String browserUsed = record.get("browserUsed");
                String content = record.get("content");
                String replyToPostId = record.get("reply_to_postId");
                String replyToCommentId = record.get("reply_to_commentId");
                String placeId = record.get("placeId");

                if(time > convertToEpoch(creationDate, 0)) {
                    System.out.println("RACKET NOT SORTED");
                    time = convertToEpoch(creationDate, 0);
                } else {
                    time = convertToEpoch(creationDate, 0);
                }
                csvPrinter.printRecord(convertToEpoch(creationDate, 0),
                        id,
                        personId,
                        creationDate,
                        locationIP,
                        browserUsed,
                        content,
                        replyToPostId,
                        replyToCommentId,
                        placeId);
            }

            csvPrinter.flush();

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(reader != null) {
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

            long time = 0;

            for(CSVRecord record : csvParser) {
                String personId = record.get("Person.id");
                String postId = record.get("Post.id");
                String creationDate = record.get("creationDate");

                if(time > convertToEpoch(creationDate, 1)) {
                    System.out.println("RACKET NOT SORTED");
                    time = convertToEpoch(creationDate, 1);
                } else {
                    time = convertToEpoch(creationDate, 1);
                }
                csvPrinter.printRecord(convertToEpoch(creationDate, 1), personId, postId, creationDate);
            }

            csvPrinter.flush();

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(reader != null) {
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

            long time = 0;

            for(CSVRecord record : csvParser) {
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

                if(time > convertToEpoch(creationDate, 0)) {
                    System.out.println("RACKET NOT SORTED");
                    time = convertToEpoch(creationDate, 0);
                } else {
                    time = convertToEpoch(creationDate, 0);
                }
                csvPrinter.printRecord(convertToEpoch(creationDate, 0),
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
            }

            csvPrinter.flush();

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
