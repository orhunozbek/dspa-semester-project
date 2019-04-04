package model;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@JsonSerialize
public class LikeEvent implements CSVReadable{

    @JsonProperty("timeMilisecond")
    private long timeMilisecond;

    @JsonProperty("personId")
    private int personId;

    @JsonProperty("postId")
    private long postId;

    @JsonProperty("creationDate")
    private LocalDateTime creationDate;

    public long getTimeMilisecond() {
        return timeMilisecond;
    }

    public int getPersonId() {
        return personId;
    }

    public long getPostId() {
        return postId;
    }

    public LocalDateTime getCreationDate() {
        return creationDate;
    }

    @Override
    public LikeEvent fromCSVRecord(CSVRecord record) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

        this.timeMilisecond = Long.parseLong(record.get("timeMilisecond"));
        this.postId = Long.parseLong(record.get("Post.id"));
        this.personId = Integer.parseInt(record.get("Person.id"));
        this.creationDate = LocalDateTime.parse(record.get("creationDate"),formatter);

        return this;
    }

    @Override
    public String getTopicName() {
        return "likes";
    }
}
