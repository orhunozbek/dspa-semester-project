package model.stream;

public interface Event {

    String getTopicName();

    long getTimestamp();
}
