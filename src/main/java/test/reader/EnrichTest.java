package test.reader;

import main.Main;
import org.apache.commons.configuration2.Configuration;
import org.junit.Test;
import preparation.Enricher;
import preparation.StreamDataPreparation;

import java.io.File;

public class EnrichTest {
    private Configuration testSetup() {
        Main.setGlobalConfig("src/main/java/test/reader/testConfig.properties");
        return Main.getGlobalConfig();
    }

    @Test
    public void convertToEpoch() {
        Enricher testEnricher = new Enricher();
        assert(testEnricher.convertToEpoch("2012-02-02T02:45:14Z", 0) == 1328147114000L);
    }

    @Test
    public void enrich() {
        Configuration configuration = testSetup();
        StreamDataPreparation streamDataPreparation = new StreamDataPreparation();
        assert(streamDataPreparation.start());
        String directory = configuration.getString("workingDirectory");
        assert((new File(directory + "/streams/post_event_stream.csv").exists()));
    }
}
