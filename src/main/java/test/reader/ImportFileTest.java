package test.reader;

import main.Main;
import org.apache.commons.configuration2.Configuration;
import org.junit.Test;

public class ImportFileTest {

    @Test
    public void testAccessProperty() {
        Main.setGlobalConfig("src/main/java/test/reader/testConfig.properties");
        Configuration configuration = Main.getGlobalConfig();
        assert(configuration.getInt("test") == 3);

    }
}
