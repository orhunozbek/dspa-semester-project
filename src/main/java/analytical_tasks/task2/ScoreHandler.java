package analytical_tasks.task2;

import main.Main;
import org.apache.commons.configuration2.Configuration;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

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

    public void merge(ScoreHandler scoreHandler) {
        for(HashMap.Entry<String,Score> entry : scoreHandler.randSelectedUserMap.entrySet()) {
            Score updateScore = randSelectedUserMap.get(entry.getKey());
            if(updateScore == null) {
                updateScore = new Score();
                updateScore.scoreUserId = entry.getKey();
                updateScore.staticScore = entry.getValue().staticScore;

            } else {
                updateScore.staticScore = updateScore.staticScore + entry.getValue().staticScore;
            }
        }
    }

    public LinkedList returnTop5() {
        LinkedList<String> result = randSelectedUserMap.entrySet()
                .stream()
                .sorted(Comparator.comparing(HashMap.Entry::getValue, new Comparator<Score>() {
                    @Override
                    public int compare(Score score, Score t1) {
                        return Float.compare(score.staticScore, t1.staticScore);
                    }
                }))
                .map(new Function<Map.Entry<String, Score>, String>() {
                    @Override
                    public String apply(Map.Entry<String, Score> stringScoreEntry) {
                        return stringScoreEntry.getKey();
                    }
                })
                .collect(Collectors.toCollection(LinkedList::new));
        if(result != null && result.size() > 5) {
            for(int i = 5; i < result.size(); i ++) {
                result.remove(i);
            }
        }
        return result;
    }


}
