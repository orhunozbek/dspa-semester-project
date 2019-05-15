package analytical_tasks.task2;

import main.Main;
import org.apache.commons.configuration2.Configuration;

import java.util.HashMap;

public class ScoreHandler {

    String randSelectedUserId;

    HashMap<String, Score> randSelectedUserMap;

    HashMap<FixedCategory, Float> weight;

    public ScoreHandler(String randSelectedUserId) {
        this.randSelectedUserId = randSelectedUserId;
        this.randSelectedUserMap = new HashMap<>();

        Configuration configuration = Main.getGlobalConfig();
        weight = new HashMap<>();
        for(FixedCategory category : FixedCategory.values()) {
            weight.put(category, configuration.getFloat(category.getConfigName()));
        }
    }

    public void updateScore(String scoreUserId, FixedCategory category) {
        Score updateScore = randSelectedUserMap.get(scoreUserId);
        if(updateScore == null) {
            updateScore = new Score();
            updateScore.scoreUserId = scoreUserId;
            updateScore.staticScore = 0 + (weight.get(category));
        } else {
            updateScore.staticScore = updateScore.staticScore + weight.get(category);
        }
        randSelectedUserMap.put(scoreUserId, updateScore);
    }

    public Score getStaticScore(String randSelectedUserId) {
        return randSelectedUserMap.get(randSelectedUserId);
    }
}
