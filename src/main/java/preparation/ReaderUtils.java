package preparation;

import main.Main;
import org.apache.commons.configuration2.Configuration;

import java.io.File;


public class ReaderUtils {

    public enum Directory {
        InputDirectory {
            public String toString() {
                return "inputDirectory";
            }
        },

        WorkingDirectory {
            public String toString() {
                return "workingDirectory";
            }
        }
    }

    public enum Topic {
        Post {
            public String toString() {
                return "/streams/post_event_stream.csv";
            }
        },

        Like {
            public String toString() {
                return "/streams/likes_event_stream.csv";
            }
        },

        Comment {
            public String toString() {
                return "/streams/comment_event_stream.csv";
            }
        },

        Forum_hasMember_person {
            public String toString() {
                return "/tables/forum_hasMember_person.csv";
            }
        },

        Forum_hasModerator_person {
            public String toString() {
                return "/tables/forum_hasModerator_person.csv";
            }
        },

        Person {
            public String toString() {
                return "/tables/person.csv";
            }
        },

        Person_hasInterest_tag {
            public String toString() {
                return "/tables/person_hasInterest_tag.csv";
            }
        },
        Person_speaks_language {
            public String toString() {
                return "/tables/person_speaks_language.csv";
            }
        },
        Person_knows_person {
            public String toString() {
                return "/tables/person_knows_person.csv";
            }
        }

    }

    public static File getFile(Directory dirName, Topic topicName) {

        Configuration configuration = Main.getGlobalConfig();
        if (configuration == null) {
            return null;
        }
        String dataDirectory = configuration.getString(dirName.toString());
        return new File(dataDirectory + topicName.toString());
    }

    public static String[] getHeaderFor(Directory dirName, Topic topic) {

        if (dirName == Directory.WorkingDirectory)
        {
        switch (topic) {
            case Comment:
                return new String[]{"timeMilisecond", "id", "personId", "creationDate", "locationIP",
                        "browserUsed", "content", "reply_to_postId", "reply_to_commentId", "placeId"};
            case Like:
                return new String[]{"timeMilisecond", "Person.id", "Post.id", "creationDate"};
            case Post:
                return new String[]{"timeMilisecond", "id", "personId", "creationDate", "imageFile",
                        "locationIP", "browserUsed", "language", "content", "tags", "forumId", "placeId"};
            default:
                return null;
        }}
        else if (dirName == Directory.InputDirectory){
            switch (topic) {
                case Comment:
                    return new String[]{"id", "personId", "creationDate", "locationIP",
                            "browserUsed", "content", "reply_to_postId", "reply_to_commentId", "placeId"};
                case Like:
                    return new String[]{"Person.id", "Post.id", "creationDate"};
                case Post:
                    return new String[]{"id", "personId", "creationDate", "imageFile",
                            "locationIP", "browserUsed", "language", "content", "tags", "forumId", "placeId"};
                default:
                    return null;
            }
        }
        else return null;
    }

}
