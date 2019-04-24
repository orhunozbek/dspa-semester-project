package model;

import org.apache.commons.csv.CSVRecord;


public interface Event {
    Event fromCSVRecord(CSVRecord record);

    String getTopicName();

    long getTimestamp();
}
