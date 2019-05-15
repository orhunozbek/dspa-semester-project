package model.stream;

import org.apache.commons.csv.CSVRecord;
import org.jetbrains.annotations.NotNull;
import preparation.ReaderUtils;

public class EventFactory {

    @NotNull
    public static Event getEventFromTopicAndRecord(ReaderUtils.Topic topic, CSVRecord record) {
        switch (topic) {
            case Post: {
                return new PostEvent(record);
            }

            case Like: {
                return new LikeEvent(record);
            }

            case Comment: {
                return new CommentEvent(record);
            }

            default:
                return null;
        }
    }
}
