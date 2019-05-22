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
        File forumHasMemberOrigFile = new File(dataDirectory + "/tables/forum_hasMember_person.csv");
        if (!forumHasMemberOrigFile.exists()) {
            return false;
        }
        File forumHasModeratorOrigFile = new File(dataDirectory + "/tables/forum_hasModerator_person.csv");
        if (!forumHasModeratorOrigFile.exists()) {
            return false;
        }
        File personOrigFile = new File(dataDirectory + "/tables/person.csv");
        if (!personOrigFile.exists()) {
            return false;
        }
        File personHasInterestOrigFile = new File(dataDirectory + "/tables/person_hasInterest_tag.csv");
        if(!personHasInterestOrigFile.exists()) {
            return false;
        }

        String workingDirectory = configuration.getString("workingDirectory");
        File workingDirectoryFile = new File(workingDirectory);
        if (!workingDirectoryFile.exists()) {
            return false;
        }
        cleanup();

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

        File tableDirectory = new File(workingDirectory + "/tables");
        tableDirectory.mkdirs();

        File forumHasMemberFile = new File(workingDirectory + "/tables/forum_hasMember_person.csv");
        try {
            forumHasMemberFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        File forumHasModeratorFile = new File(workingDirectory + "/tables/forum_hasModerator_person.csv");
        try {
            forumHasModeratorFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        File personFile = new File(workingDirectory + "/tables/person.csv");
        try {
            personFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        File personHasInterestFile = new File(workingDirectory + "/tables/person_hasInterest_tag.csv");
        try {
            personHasInterestFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Dynamic data

        Enricher enricher = new Enricher();
        enricher.enrichCommentEventStream(commentEventStreamOrigFile, commentEventStreamFile);
        enricher.enrichLikesEventStream(likesEventStreamOrigFile, likesEventStreamFile);
        enricher.enrichPostEventStream(postEventStreamOrigFile, postEventStreamFile);

        // Static data
        enricher.enrichForumHasMember(forumHasMemberOrigFile, forumHasMemberFile);
        enricher.enrichForumHasModerator(forumHasModeratorOrigFile, forumHasModeratorFile);
        enricher.enrichPerson(personOrigFile, personFile);
        enricher.enrichPersonHasInterestTag(personHasInterestOrigFile, personHasInterestFile);

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
