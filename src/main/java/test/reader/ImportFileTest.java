package test.reader;

import main.Main;
import org.apache.commons.configuration2.Configuration;
import org.junit.Test;
import preparation.StreamDataPreparation;

import java.io.File;

public class ImportFileTest {


    private Configuration testSetup() {
        Main.setGlobalConfig("src/main/java/test/reader/testConfig.properties");
        return Main.getGlobalConfig();
    }

    @Test
    public void testAccessProperty() {
        Configuration configuration = testSetup();
        assert(configuration.getInt("test") == 3);
    }


    @Test
    public void testFileStructureSetup() {
        Configuration configuration = testSetup();
        StreamDataPreparation streamDataPreparation = new StreamDataPreparation();
        assert(streamDataPreparation.start());
        String directory = configuration.getString("workingDirectory");
        assert((new File(directory + "/streams/post_event_stream.csv").exists()));
        ///assert(streamDataPreparation.cleanup());
        assert(!(new File(directory + "/streams/post_event_stream.csv").exists()));
    }

}
