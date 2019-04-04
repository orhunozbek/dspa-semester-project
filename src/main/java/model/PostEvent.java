package model;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import org.apache.commons.lang3.math.NumberUtils;

@JsonSerialize
public class PostEvent implements CSVReadable{

    @JsonProperty("timeMilisecond")
    private long timeMilisecond;

    @JsonProperty("id")
    private long id;

    @JsonProperty("personId")
    private int personId;

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
    private int[] tags;

    @JsonProperty("forumId")
    private int forumId;

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

    public int[] getTags() {
        return tags;
    }

    public int getForumId() {
        return forumId;
    }

    public int getPlaceId() {
        return placeId;
    }

    @Override
    public PostEvent fromCSVRecord(CSVRecord record) {

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

        this.timeMilisecond = Long.parseLong(record.get("timeMilisecond"));
        this.id = Long.parseLong(record.get("id"));
        this.personId = Integer.parseInt(record.get("personId"));
        this.creationDate = LocalDateTime.parse(record.get("creationDate"),formatter);
        this.imageFile = record.get("imageFile");
        this.locationIP = record.get("locationIP");
        this.browserUsed = record.get("browserUsed");
        this.language = record.get("language");
        this.content = record.get("content");
        this.tags = Arrays.stream(record.get("tags").split("\\D+")).mapToInt(NumberUtils::toInt).toArray();
        this.forumId = Integer.parseInt(record.get("forumId"));
        this.placeId = Integer.parseInt(record.get("placeId"));

        return this;
    }

    @Override
    public String getTopicName() {
        return "posts";
    }
}
