package main;

import kafka.EventKafkaProducer;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import preparation.StreamDataPreparation;
import thread.CommentThread;
import thread.LikeThread;
import thread.PostThread;

import java.io.File;

import static preparation.ReaderUtils.Topic.*;

public class Main {
    public static final String DEFAULT_CONFIG_LOCATION = "config.properties";

    private static Configurations configs;
    private static String location = "";

    public static void main(String[] args) {
        setGlobalConfig(DEFAULT_CONFIG_LOCATION);

        boolean writeToKafka = Main.getGlobalConfig().getBoolean("writeToKafka");

        if(writeToKafka) {
            StreamDataPreparation streamDataPreparation = new StreamDataPreparation();
            streamDataPreparation.start();

            EventKafkaProducer.streamToKafka(Comment);
            EventKafkaProducer.streamToKafka(Like);
            EventKafkaProducer.streamToKafka(Post);
        }

        CommentThread commentThread = new CommentThread();
        commentThread.start();

        PostThread postThread = new PostThread();
        postThread.start();

        LikeThread likeThread = new LikeThread();
        likeThread.start();

    }

    public static void setGlobalConfig(String s) {
        if(configs == null) {
            configs = new Configurations();
        }
        location = s;
    }

    public static Configuration getGlobalConfig() {
        if(configs == null) {
            System.out.println("Configurations not set! Call setGlobalConfig first");
            return null;
        }
        try {
            return configs.properties(new File(location));
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
        return null;
    }
}
