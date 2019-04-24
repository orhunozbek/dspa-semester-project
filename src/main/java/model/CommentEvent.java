package model;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@JsonSerialize
public class CommentEvent implements Event {

    @JsonProperty("timeMilisecond")
    private long timeMilisecond;

    @JsonProperty("id")
    private String id;

    @JsonProperty("personId")
    private String personId;

    @JsonProperty("creationDate")
    private LocalDateTime creationDate;

    @JsonProperty("locationIP")
    private String locationIP;

    @JsonProperty("browserUsed")
    private String browserUsed;

    @JsonProperty("content")
    private String content;

    @JsonProperty("reply_to_postId")
    private String reply_to_postId;

    @JsonProperty("reply_to_commentId")
    private String reply_to_commentId;

    @JsonProperty("placeId")
    private String placeId;

    public long getTimeMilisecond() {
        return timeMilisecond;
    }

    public String getId() {
        return id;
    }

    public String getPersonId() {
        return personId;
    }

    public LocalDateTime getCreationDate() {
        return creationDate;
    }

    public String getLocationIP() {
        return locationIP;
    }

    public String getBrowserUsed() {
        return browserUsed;
    }

    public String getContent() {
        return content;
    }

    public String getReply_to_postId() {
        return reply_to_postId;
    }

    public String getReply_to_commentId() {
        return reply_to_commentId;
    }

    public String getPlaceId() {
        return placeId;
    }

    private CommentEvent() {
    }

    public CommentEvent(long timeMilisecond, String id, String personId, LocalDateTime creationDate, String locationIP, String browserUsed, String content, String reply_to_postId, String reply_to_commentId, String placeId) {
        this.timeMilisecond = timeMilisecond;
        this.id = id;
        this.personId = personId;
        this.creationDate = creationDate;
        this.locationIP = locationIP;
        this.browserUsed = browserUsed;
        this.content = content;
        this.reply_to_postId = reply_to_postId;
        this.reply_to_commentId = reply_to_commentId;
        this.placeId = placeId;
    }

    public CommentEvent(CSVRecord record) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

        this.timeMilisecond = Long.parseLong(record.get("timeMilisecond"));
        this.id = record.get("id");
        this.personId = record.get("personId");
        this.creationDate = LocalDateTime.parse(record.get("creationDate"),formatter);
        this.locationIP = record.get("locationIP");
        this.browserUsed = record.get("browserUsed");
        this.content = record.get("content");
        this.reply_to_postId = record.get("reply_to_postId");
        this.reply_to_commentId = record.get("reply_to_commentId");
        this.placeId = record.get("placeId");
    }

    @Override
    public String getTopicName() {
        return "comments";
    }

    @Override
    public long getTimestamp() {
        return timeMilisecond;
    }
}
