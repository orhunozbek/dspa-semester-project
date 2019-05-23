package preparation;

import main.Main;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

import static preparation.ReaderUtils.getFile;

public class StreamDataPreparation {

    public boolean start() {
        Configuration configuration = Main.getGlobalConfig();

        if(configuration == null) return false;

        ReaderUtils.Topic[] topics = ReaderUtils.Topic.values();
        String workingDirectory = configuration.getString(ReaderUtils.Directory.WorkingDirectory.toString());
        Enricher enricher = new Enricher();

        try {
            setupWorkingDirectory(workingDirectory);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        for (ReaderUtils.Topic topic : topics){

            File inputFile = getFile(ReaderUtils.Directory.InputDirectory, topic);
            File outputFile = getFile(ReaderUtils.Directory.WorkingDirectory, topic);
            if (inputFile == null || outputFile == null) return false;

            enricher.enrichStream(topic, inputFile, outputFile);
        }

        return true;
    }

    private void setupWorkingDirectory(String workingDir) throws IOException {

        File workingDirectoryFile = new File(workingDir);

        if (!workingDirectoryFile.exists()) {
            FileUtils.forceMkdir(workingDirectoryFile);
        } else{
            FileUtils.cleanDirectory(workingDirectoryFile);
        }

        File streamsDirectory = new File(workingDir + "/streams");
        FileUtils.forceMkdir(streamsDirectory);

        File tablesDirectory = new File(workingDir + "/tables");
        FileUtils.forceMkdir(tablesDirectory);
    }

}
