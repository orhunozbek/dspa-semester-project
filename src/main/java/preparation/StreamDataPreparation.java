package preparation;

import main.Main;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class StreamDataPreparation {
    public boolean start() {
        Configuration configuration = Main.getGlobalConfig();
        if(configuration == null) {
            return false;
        }
        String dataDirectory = configuration.getString("inputDirectory");
        File commentEventStreamOrigFile = new File(dataDirectory + "/streams/comment_event_stream.csv");
        if (!commentEventStreamOrigFile.exists()) {
            return false;
        }
        File likesEventStreamOrigFile = new File(dataDirectory + "/streams/likes_event_stream.csv");
        if (!likesEventStreamOrigFile.exists()) {
            return false;
        }
        File postEventStreamOrigFile = new File(dataDirectory + "/streams/post_event_stream.csv");
        if (!postEventStreamOrigFile.exists()) {
            return false;
        }

        String workingDirectory = configuration.getString("workingDirectory");
        File workingDirectoryFile = new File(workingDirectory);
        if (!workingDirectoryFile.exists()) {
            return false;
        }

        File streamsDirectory = new File(workingDirectory + "/streams");
        streamsDirectory.mkdirs();

        File commentEventStreamFile = new File(workingDirectory + "/streams/comment_event_stream.csv");
        try {
            commentEventStreamFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        File likesEventStreamFile = new File(workingDirectory + "/streams/likes_event_stream.csv");
        try {
            likesEventStreamFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        File postEventStreamFile = new File(workingDirectory + "/streams/post_event_stream.csv");
        try {
            postEventStreamFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return true;
    }

    public boolean cleanup() {
        Configuration configuration = Main.getGlobalConfig();
        if(configuration == null) {
            return false;
        }

        String workingDirectory = configuration.getString("workingDirectory");
        File workingDirectoryFile = new File(workingDirectory);
        if (!workingDirectoryFile.exists()) {
            return false;
        }

        try {
            FileUtils.cleanDirectory(workingDirectoryFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }
}
