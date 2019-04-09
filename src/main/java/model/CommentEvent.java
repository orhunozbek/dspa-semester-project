package model;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.commons.lang3.math.NumberUtils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@JsonSerialize
public class CommentEvent extends Event implements CSVReadable{

    @JsonProperty("timeMilisecond")
    private long timeMilisecond;

    @JsonProperty("id")
    private long id;

    @JsonProperty("personId")
    private int personId;

    @JsonProperty("creationDate")
    private LocalDateTime creationDate;

    @JsonProperty("locationIP")
    private String locationIP;

    @JsonProperty("browserUsed")
    private String browserUsed;

    @JsonProperty("content")
    private String content;

    @JsonProperty("reply_to_postId")
    private long reply_to_postId;

    @JsonProperty("reply_to_commentId")
    private long reply_to_commentId;

    @JsonProperty("placeId")
    private int placeId;

    public long getTimeMilisecond() {
        return timeMilisecond;
    }

    public long getId() {
        return id;
    }

    public int getPersonId() {
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

    public long getReply_to_postId() {
        return reply_to_postId;
    }

    public long getReply_to_commentId() {
        return reply_to_commentId;
    }

    public int getPlaceId() {
        return placeId;
    }

    @Override
    public CommentEvent fromCSVRecord(CSVRecord record) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

        this.timeMilisecond = Long.parseLong(record.get("timeMilisecond"));
        this.id = Long.parseLong(record.get("id"));
        this.personId = Integer.parseInt(record.get("personId"));
        this.creationDate = LocalDateTime.parse(record.get("creationDate"),formatter);
        this.locationIP = record.get("locationIP");
        this.browserUsed = record.get("browserUsed");
        this.content = record.get("content");
        this.reply_to_postId = NumberUtils.toLong(record.get("reply_to_postId"));
        this.reply_to_commentId = NumberUtils.toLong(record.get("reply_to_commentId"));
        this.placeId = Integer.parseInt(record.get("placeId"));

        return this;
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
