package analytical_tasks.task3;

import main.Main;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;
import java.util.*;

import static main.Main.setGlobalConfig;

class Task3_Metrics {

     private static final String DEFAULT_CONFIG_LOCATION = "config.properties";
     private static HashSet<String> badWords;

     static{
         // Code in order to load "bad word list" for profanity filtering

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

     static Double calculateUniqueWordsOverWordsMetric(String post){
        String[] result = post.split("\\s");
        double uniqueCount;
        double wordCount = result.length;

        HashSet<String> uniqueWords = new HashSet<String>(Arrays.asList(result));
        uniqueCount = uniqueWords.size();

        return uniqueCount/wordCount;
    }

    static Map<String,Integer> vectorize(String post){
         String[] result = post.split("\\s");
         Map<String, Integer> distinctWords = new HashMap<String, Integer>();

         for (String word : result){
             if (distinctWords.containsKey(word)){
                 distinctWords.put(word, distinctWords.get(word) +1);
             }else{
                 distinctWords.put(word,1);
             }
         }

         return distinctWords;
    }

    static Double countBadWords (String post){
        String[] result = post.split("\\s");
        Double numBadWords = 0.0;

        for (String word: result){
            if ( badWords.contains(word.toLowerCase())){
                numBadWords ++;
            }
        }

        return numBadWords;
    }


}
