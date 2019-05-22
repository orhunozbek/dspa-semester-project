package analytical_tasks.task2;

import main.Main;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import java.io.*;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;

import static analytical_tasks.task2.FixedCategory.*;

/**
 * This class handles the static score calculation before
 * other calculations happen. It means that this is executed
 * before stream starts. In a real world application this would
 * be done maybe once a day.
 */
public class StaticScoreCalculator {

    private ScoreHandler[] scoreList;
    private String[] selectedUserIdArray;

    private Set<String>[] selectedUserForums;
    private Set<String>[] selectedUserModeratorForums;
    private int[] birthdays;
    private String[] browsers;
    private Set<String>[] interests;
    private Set<String>[] languages;

    public StaticScoreCalculator() {
        scoreList = new ScoreHandler[10];
        selectedUserIdArray = new String[10];

        Configuration configuration = Main.getGlobalConfig();
        scoreList[0] = new ScoreHandler(configuration.getString("friendId0"));
        selectedUserIdArray[0] = configuration.getString("friendId0");
        scoreList[1] = new ScoreHandler(configuration.getString("friendId1"));
        selectedUserIdArray[1] = configuration.getString("friendId1");
        scoreList[2] = new ScoreHandler(configuration.getString("friendId2"));
        selectedUserIdArray[2] = configuration.getString("friendId2");
        scoreList[3] = new ScoreHandler(configuration.getString("friendId3"));
        selectedUserIdArray[3] = configuration.getString("friendId3");
        scoreList[4] = new ScoreHandler(configuration.getString("friendId4"));
        selectedUserIdArray[4] = configuration.getString("friendId4");
        scoreList[5] = new ScoreHandler(configuration.getString("friendId5"));
        selectedUserIdArray[5] = configuration.getString("friendId5");
        scoreList[6] = new ScoreHandler(configuration.getString("friendId6"));
        selectedUserIdArray[6] = configuration.getString("friendId6");
        scoreList[7] = new ScoreHandler(configuration.getString("friendId7"));
        selectedUserIdArray[7] = configuration.getString("friendId7");
        scoreList[8] = new ScoreHandler(configuration.getString("friendId8"));
        selectedUserIdArray[8] = configuration.getString("friendId8");
        scoreList[9] = new ScoreHandler(configuration.getString("friendId9"));
        selectedUserIdArray[9] = configuration.getString("friendId9");

        selectedUserForums = new Set[10];
        selectedUserModeratorForums = new Set[10];
        birthdays = new int[10];
        browsers = new String[10];
        interests = new Set[10];
        languages = new Set[10];
    }

    // Reads static scores from tables files
    public ScoreHandler[] readStaticScores() throws Exception {
        Configuration configuration = Main.getGlobalConfig();
        String workingDirectory = configuration.getString("workingDirectory");
        File workingDirectoryFile = new File(workingDirectory);
        if (!workingDirectoryFile.exists()) {
            throw new FileNotFoundException("Working Directory not Found");
        }

        forumMemberModerater(workingDirectory);

        userSimilarities(configuration, workingDirectory);

        personHasInterest(workingDirectory);

        personSpeaksLanguage(workingDirectory);

        return scoreList;
    }

    // Increase score, if they speak the same language
    private void personSpeaksLanguage(String workingDirectory) throws IOException {
        File personSpeaksLanguage = new File(workingDirectory + "/tables/person_speaks_language.csv");
        if(!personSpeaksLanguage.exists()) {
            throw new FileNotFoundException("Language file not found");
        }

        Reader reader = Files.newBufferedReader(personSpeaksLanguage.toPath());
        CSVFormat inputFormat = CSVFormat.newFormat('|')
                .withHeader("Person.id", "language")
                .withFirstRecordAsHeader()
                .withRecordSeparator('\n');
        CSVParser csvParser = new CSVParser(reader, inputFormat);
        for(CSVRecord record : csvParser) {
            String personId = record.get("Person.id");
            String language = record.get("language");

            int index = getIndexFromSelectedUserId(personId);
            if (index != -1) {
                if(languages[index] == null) {
                    languages[index] = new HashSet<>();
                }
                languages[index].add(language);
            }
        }
        reader.close();

        reader = Files.newBufferedReader(personSpeaksLanguage.toPath());
        inputFormat = CSVFormat.newFormat('|')
                .withHeader("Person.id", "language")
                .withFirstRecordAsHeader()
                .withRecordSeparator('\n');
        csvParser = new CSVParser(reader, inputFormat);
        for(CSVRecord record : csvParser) {
            String personId = record.get("Person.id");
            String language = record.get("language");

            for(int i = 0; i < 10; i++) {
                if(languages[i].contains(language)) {
                    scoreList[i].updateScore(personId, SAME_LANGUAGE);
                }
            }
        }
        reader.close();
    }

    //Increase score if they have the same interests.
    private void personHasInterest(String workingDirectory) throws IOException {
        File personHasInterestTag = new File(workingDirectory + "/tables/person_hasInterest_tag.csv");
        if (!personHasInterestTag.exists()) {
            throw new FileNotFoundException("Person File not found");
        }

        Reader reader = Files.newBufferedReader(personHasInterestTag.toPath());
        CSVFormat inputFormat = CSVFormat.newFormat('|')
                .withHeader("Person.id", "Tag.id")
                .withFirstRecordAsHeader()
                .withRecordSeparator('\n');
        CSVParser csvParser = new CSVParser(reader, inputFormat);
        for(CSVRecord record : csvParser) {
            String personId = record.get("Person.id");
            String tagId = record.get("Tag.id");

            int index = getIndexFromSelectedUserId(personId);
            if (index != -1) {
                if(interests[index] == null) {
                    interests[index] = new HashSet<>();
                }
                interests[index].add(tagId);
            }
        }
        reader.close();

        reader = Files.newBufferedReader(personHasInterestTag.toPath());
        inputFormat = CSVFormat.newFormat('|')
                .withHeader("Person.id", "Tag.id")
                .withFirstRecordAsHeader()
                .withRecordSeparator('\n');
        csvParser = new CSVParser(reader, inputFormat);
        for(CSVRecord record : csvParser) {
            String personId = record.get("Person.id");
            String tagId = record.get("Tag.id");

            for(int i = 0; i < 10; i++) {
                if(interests[i].contains(tagId)) {
                    scoreList[i].updateScore(personId, SAME_INTEREST_TAG);
                }
            }
        }
        reader.close();
    }

    //Increase the score if they are of the same age or use the same browser
    private void userSimilarities(Configuration configuration, String workingDirectory) throws IOException {
        int sameAgeRange = configuration.getInt("sameAgeRange");
        File person = new File(workingDirectory + "/tables/person.csv");
        if (!person.exists()) {
            throw new FileNotFoundException("Person File not found");
        }

        Reader reader = Files.newBufferedReader(person.toPath());
        CSVFormat inputFormat = CSVFormat.newFormat('|')
                .withHeader("id", "firstName", "lastName", "gender", "birthday",
                        "creationDate", "locationIP", "browserUsed")
                .withFirstRecordAsHeader()
                .withRecordSeparator('\n');
        CSVParser csvParser = new CSVParser(reader, inputFormat);
        for(CSVRecord record : csvParser) {
            String id = record.get("id");
            String birthday = record.get("birthday");
            String browser = record.get("browserUsed");

            int index = getIndexFromSelectedUserId(id);
            if (index != -1) {
                birthdays[index] = extractBirthdayYear(birthday);
                browsers[index] = browser;
            }
        }
        reader.close();

        reader = Files.newBufferedReader(person.toPath());
        inputFormat = CSVFormat.newFormat('|')
                .withHeader("id", "firstName", "lastName", "gender", "birthday",
                        "creationDate", "locationIP", "browserUsed")
                .withFirstRecordAsHeader()
                .withRecordSeparator('\n');
        csvParser = new CSVParser(reader, inputFormat);
        for(CSVRecord record : csvParser) {
            String id = record.get("id");
            String birthday = record.get("birthday");
            String browser = record.get("browserUsed");

            for(int i = 0; i < 10; i++) {
                if(Math.abs(extractBirthdayYear(birthday) - birthdays[i]) <= sameAgeRange) {
                    scoreList[i].updateScore(id, SAME_AGE);
                }

                if(browser.equals(browsers[i])) {
                    scoreList[i].updateScore(id, SAME_BROWSER);
                }
            }
        }
        reader.close();
    }

    // Increase score if they use the same forum.
    private void forumMemberModerater(String workingDirectory) throws IOException {
        // Same Forum Member/Moderator
        File forumHasMember = new File(workingDirectory + "/tables/forum_hasMember_person.csv");
        if (!forumHasMember.exists()) {
            throw new FileNotFoundException("Forum Has Member File not Found");
        }
        File forumHasModerator = new File(workingDirectory + "/tables/forum_hasModerator_person.csv");
        if (!forumHasModerator.exists()) {
            throw new FileNotFoundException("Forum Has Moderator File not Found");
        }

        Reader reader = Files.newBufferedReader(forumHasMember.toPath());
        CSVFormat inputFormat = CSVFormat.newFormat('|')
                .withHeader("Forum.id", "Person.id", "joinDate")
                .withFirstRecordAsHeader()
                .withRecordSeparator('\n');
        CSVParser csvParser = new CSVParser(reader, inputFormat);
        for(CSVRecord record : csvParser) {
            String forumId = record.get("Forum.id");
            String personId = record.get("Person.id");

            int index = getIndexFromSelectedUserId(personId);
            if (index != -1) {
                if(selectedUserForums[index] == null) {
                    selectedUserForums[index] = new HashSet<>();
                }
                selectedUserForums[index].add(forumId);
            }
        }
        reader.close();

        reader = Files.newBufferedReader(forumHasModerator.toPath());
        inputFormat = CSVFormat.newFormat('|')
                .withHeader("Forum.id", "Person.id")
                .withFirstRecordAsHeader()
                .withRecordSeparator('\n');
        csvParser = new CSVParser(reader, inputFormat);
        for(CSVRecord record : csvParser) {
            String forumId = record.get("Forum.id");
            String personId = record.get("Person.id");

            int index = getIndexFromSelectedUserId(personId);
            if (index != -1) {
                if(selectedUserModeratorForums[index] == null) {
                    selectedUserModeratorForums[index] = new HashSet<>();
                }
                selectedUserModeratorForums[index].add(forumId);
            }
        }
        reader.close();

        reader = Files.newBufferedReader(forumHasMember.toPath());
        csvParser = new CSVParser(reader, inputFormat);
        for(CSVRecord record : csvParser) {
            String forumId = record.get("Forum.id");
            String personId = record.get("Person.id");

            for(int i = 0; i < 10; i++) {
                if(selectedUserForums[i].contains(forumId)) {
                    scoreList[i].updateScore(personId, SAME_FORUM_MEMBER);
                }
            }

            for(int i = 0; i < 10; i++) {
                if(selectedUserModeratorForums[i].contains(forumId)) {
                    scoreList[i].updateScore(personId, SAME_FORUM_MEMBER_MODERATOR);
                }
            }
        }

        reader = Files.newBufferedReader(forumHasModerator.toPath());
        csvParser = new CSVParser(reader, inputFormat);
        for(CSVRecord record : csvParser) {
            String forumId = record.get("Forum.id");
            String personId = record.get("Person.id");

            for(int i = 0; i < 10; i++) {
                if(selectedUserModeratorForums[i].contains(forumId)) {
                    scoreList[i].updateScore(personId, SAME_FORUM_MODERATOR);
                }
            }

            for(int i = 0; i < 10; i++) {
                if(selectedUserForums[i].contains(forumId)) {
                    scoreList[i].updateScore(personId, SAME_FORUM_MEMBER_MODERATOR);
                }
            }
        }
        reader.close();
    }

    private int getIndexFromSelectedUserId(String selectedUserId) {
        for(int i = 0; i < 10; i++) {
            if(selectedUserId.equals(selectedUserIdArray[i])) {
                return i;
            }
        }
        return -1;
    }

    private int extractBirthdayYear(String birthday) {
        return Integer.valueOf(birthday.substring(0,4));
    }
}
