package model;

import org.apache.commons.csv.CSVRecord;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@JsonSerialize
public class LikeEvent extends Event implements HasUserID {

    @JsonProperty("timeMilisecond")
    private long timeMilisecond;

    @JsonProperty("personId")
    private String personId;

    @JsonProperty("postId")
    private String postId;

    @JsonProperty("creationDate")
    private LocalDateTime creationDate;

    public long getTimeMilisecond() {
        return timeMilisecond;
    }

    public String getPersonId() {
        return personId;
    }

    public String getPostId() {
        return postId;
    }

    public LocalDateTime getCreationDate() {
        return creationDate;
    }

    private LikeEvent() {
    }

    public LikeEvent(long timeMilisecond, String personId, String postId, LocalDateTime creationDate) {
        this.timeMilisecond = timeMilisecond;
        this.personId = personId;
        this.postId = postId;
        this.creationDate = creationDate;
    }

    public LikeEvent(CSVRecord record) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

        this.timeMilisecond = Long.parseLong(record.get("timeMilisecond"));
        this.postId = record.get("Post.id");
        this.personId = record.get("Person.id");
        this.creationDate = LocalDateTime.parse(record.get("creationDate"),formatter);

    }

    @Override
    public String getTopicName() {
        return "likes";
    }

    @Override
    public long getTimestamp() {
        return timeMilisecond;
    }

    @Override
    public String getUserId() {
        return personId;
    }
}
