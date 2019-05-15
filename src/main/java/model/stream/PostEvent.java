package model.stream;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@JsonSerialize
public class PostEvent implements Event {

    @JsonProperty("timeMilisecond")
    private long timeMilisecond;

    @JsonProperty("id")
    private String id;

    @JsonProperty("personId")
    private String personId;

    @JsonProperty("creationDate")
    private LocalDateTime creationDate;

    @JsonProperty("imageFile")
    private String imageFile;

    @JsonProperty("locationIP")
    private String locationIP;

    @JsonProperty("browserUsed")
    private String browserUsed;

    @JsonProperty("language")
    private String language;

    @JsonProperty("content")
    private String content;

    @JsonProperty("tags")
    private String[] tags;

    @JsonProperty("forumId")
    private String forumId;

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

    public String getImageFile() {
        return imageFile;
    }

    public String getLocationIP() {
        return locationIP;
    }

    public String getBrowserUsed() {
        return browserUsed;
    }

    public String getLanguage() {
        return language;
    }

    public String getContent() {
        return content;
    }

    public String[] getTags() {
        return tags;
    }

    public String getForumId() {
        return forumId;
    }

    public String getPlaceId() {
        return placeId;
    }

    private PostEvent() {
    }

    public PostEvent(long timeMilisecond, String id, String personId, LocalDateTime creationDate, String imageFile, String locationIP, String browserUsed, String language, String content, String[] tags, String forumId, String placeId) {
        this.timeMilisecond = timeMilisecond;
        this.id = id;
        this.personId = personId;
        this.creationDate = creationDate;
        this.imageFile = imageFile;
        this.locationIP = locationIP;
        this.browserUsed = browserUsed;
        this.language = language;
        this.content = content;
        this.tags = tags;
        this.forumId = forumId;
        this.placeId = placeId;
    }

    public PostEvent(CSVRecord record) {

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

        this.timeMilisecond = Long.parseLong(record.get("timeMilisecond"));
        this.id = record.get("id");
        this.personId = record.get("personId");
        this.creationDate = LocalDateTime.parse(record.get("creationDate"),formatter);
        this.imageFile = record.get("imageFile");
        this.locationIP = record.get("locationIP");
        this.browserUsed = record.get("browserUsed");
        this.language = record.get("language");
        this.content = record.get("content");
        this.tags = record.get("tags").split("\\D+");
        this.forumId = record.get("forumId");
        this.placeId = record.get("placeId");

    }

    @Override
    public String getTopicName() {
        return "posts";
    }

    @Override
    public long getTimestamp() {
        return timeMilisecond;
    }
}
