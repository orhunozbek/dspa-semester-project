package analytical_tasks.task2;

import main.Main;

public class Task2 {

    public static void main(String[] args) {
        Main.setGlobalConfig(Main.DEFAULT_CONFIG_LOCATION);
        // Read static data and calculate static score.
        StaticScoreCalculator staticScoreCalculator = new StaticScoreCalculator();
        try {
            staticScoreCalculator.readStaticScores();
        } catch (Exception e) {
            System.out.println("Static Score Calculation failed.");
            e.printStackTrace();
            return;
        }
    }
}
