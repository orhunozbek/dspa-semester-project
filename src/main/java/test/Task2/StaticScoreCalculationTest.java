package test.Task2;

import analytical_tasks.task2.ScoreHandler;
import analytical_tasks.task2.StaticScoreCalculator;
import main.Main;
import org.junit.Test;

public class StaticScoreCalculationTest {

    @Test
    public void runStaticScoreCalculation() {
        Main.setGlobalConfig(Main.DEFAULT_CONFIG_LOCATION);
        // Read static data and calculate static score.
        StaticScoreCalculator staticScoreCalculator = new StaticScoreCalculator();
        try {
            staticScoreCalculator.readStaticScores();
        } catch (Exception e) {
            System.out.println("Static Score Calculation failed.");
            e.printStackTrace();
        }
    }

    private ScoreHandler[] staticScores;

    @Test
    public void checkSomeResults() {
        Main.setGlobalConfig(Main.DEFAULT_CONFIG_LOCATION);
        // Read static data and calculate static score.
        StaticScoreCalculator staticScoreCalculator = new StaticScoreCalculator();
        try {
            staticScores = staticScoreCalculator.readStaticScores();
        } catch (Exception e) {
            System.out.println("Static Score Calculation failed.");
            e.printStackTrace();
        }


    }
}
