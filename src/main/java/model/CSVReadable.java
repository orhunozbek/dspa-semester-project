package model;

import org.apache.commons.csv.CSVRecord;


public interface CSVReadable {
    CSVReadable fromCSVRecord(CSVRecord record);
    String getTopicName();
}
