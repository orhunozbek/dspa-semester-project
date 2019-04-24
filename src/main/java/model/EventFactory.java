package model;

import org.apache.commons.csv.CSVRecord;
import org.jetbrains.annotations.NotNull;
import preparation.ReaderUtils;

public class EventFactory {

    @NotNull
    public static Event getEventFromTopicAndRecord(ReaderUtils.Topic topic, CSVRecord record) {
        switch (topic) {
            case Post: {
                PostEvent event = new PostEvent();
                return event.fromCSVRecord(record);
            }

            case Like: {
                LikeEvent event = new LikeEvent();
                return event.fromCSVRecord(record);
            }

            case Comment: {
                CommentEvent event = new CommentEvent();
                return event.fromCSVRecord(record);
            }

            default:
                return null;
        }
    }
}
