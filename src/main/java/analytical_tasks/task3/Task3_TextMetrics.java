package analytical_tasks.task3;

import main.Main;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;
import java.util.*;

import static main.Main.setGlobalConfig;

/**
 * A class to calculate the text metrics for post and comments.
 */
class Task3_TextMetrics {

    private static final String DEFAULT_CONFIG_LOCATION = "config.properties";
    private static HashSet<String> badWords;

    static {
        // Load badWords set from JSON file
        setGlobalConfig(DEFAULT_CONFIG_LOCATION);
        org.apache.commons.configuration2.Configuration configs = Main.getGlobalConfig();
        assert configs != null;

        String badWordsDirectory = configs.getString("badWordsDirectory");

        try {
            JSONObject obj = (JSONObject) new JSONParser().parse(new FileReader(badWordsDirectory));
            badWords = new HashSet<>();
            JSONArray badWordsArray = (JSONArray) obj.get("BAD_WORDS");

            // iterating badWordsArray
            Iterator iter = badWordsArray.iterator();

            for (; iter.hasNext(); ) {
                String bw = (String) iter.next();
                badWords.add(bw);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /***
     * Calculates the proportion of unique words over the number of words in a sentence. If this metric is low,
     * it indicates that the user uses a word repetitively, and possibly the message is a spam.
     *
     * @param post String to calculate the text metric
     * @return #(unique words) / #(words) for a sentence
     */
    static Double calculateUniqueWordsOverWordsMetric(String post) {
        String[] result = post.split("\\s");
        double uniqueCount;
        double wordCount = result.length;

        HashSet<String> uniqueWords = new HashSet<String>(Arrays.asList(result));
        uniqueCount = uniqueWords.size();

        return uniqueCount / wordCount;
    }

    /**
     * Counts each distinct word in a sentence.
     *
     * @param post String to calculate the text metric
     * @return A map which contains word counts of the sentence
     */
    static Map<String, Integer> vectorize(String post) {
        String[] result = post.split("\\s");
        Map<String, Integer> distinctWords = new HashMap<String, Integer>();

        for (String word : result) {
            if (distinctWords.containsKey(word)) {
                distinctWords.put(word, distinctWords.get(word) + 1);
            } else {
                distinctWords.put(word, 1);
            }
        }

        return distinctWords;
    }

    /**
     * Calculates the number of bad words in a sentence.
     *
     * @param post String to calculate the text metric
     * @return #(badWords) in a sentence
     */
    static Double countBadWords(String post) {
        String[] result = post.split("\\s");
        Double numBadWords = 0.0;

        for (String word : result) {
            if (badWords.contains(word.toLowerCase())) {
                numBadWords++;
            }
        }

        return numBadWords;
    }


}
