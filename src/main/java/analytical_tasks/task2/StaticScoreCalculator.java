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

public class StaticScoreCalculator {

    private ScoreHandler[] scoreList;
    private String[] selectedUserIdArray;

    private Set<String>[] selectedUserForums;
    private Set<String>[] selectedUserModeratorForums;
    private int[] birthdays;

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
    }

    public ScoreHandler[] readStaticScores() throws Exception {
        Configuration configuration = Main.getGlobalConfig();
        String workingDirectory = configuration.getString("workingDirectory");
        File workingDirectoryFile = new File(workingDirectory);
        if (!workingDirectoryFile.exists()) {
            throw new FileNotFoundException("Working Directory not Found");
        }

        forumMemberModerater(workingDirectory);

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

            int index = getIndexFromSelectedUserId(id);
            if (index != -1) {
                birthdays[index] = extractBirthdayYear(birthday);
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

            for(int i = 0; i < 10; i++) {
                if(Math.abs(extractBirthdayYear(birthday) - birthdays[i]) <= sameAgeRange) {
                    scoreList[i].updateScore(id, FixedCategory.SAME_AGE);
                }
            }
        }
        reader.close();

        return scoreList;
    }

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
                    scoreList[i].updateScore(personId, FixedCategory.SAME_FORUM_MEMBER);
                }
            }

            for(int i = 0; i < 10; i++) {
                if(selectedUserModeratorForums[i].contains(forumId)) {
                    scoreList[i].updateScore(personId, FixedCategory.SAME_FORUM_MEMBER_MODERATOR);
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
                    scoreList[i].updateScore(personId, FixedCategory.SAME_FORUM_MODERATOR);
                }
            }

            for(int i = 0; i < 10; i++) {
                if(selectedUserForums[i].contains(forumId)) {
                    scoreList[i].updateScore(personId, FixedCategory.SAME_FORUM_MEMBER_MODERATOR);
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
