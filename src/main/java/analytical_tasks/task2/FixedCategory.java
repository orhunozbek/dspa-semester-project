package analytical_tasks.task2;

public enum FixedCategory {
    SAME_FORUM_MEMBER("sameForumMember"),
    SAME_FORUM_MODERATOR("sameForumModerator"),
    SAME_FORUM_MEMBER_MODERATOR("sameForumMemberModerator"),
    SAME_AGE("sameAge"),
    SAME_BROWSER("sameBrowser"),
    SAME_INTEREST_TAG("sameInterestTag"),
    SAME_LOCATION("sameLocation"),
    SAME_LANGUAGE("sameLanguage"),
    SAME_STUDY_PLACE("sameStudyPlace"),
    SAME_STUDY_PLACE_AND_YEAR("sameStudyPlaceAndYear"),
    SAME_WORK_PLACE("sameWorkPlace"),
    SAME_WORK_PLACE_AND_YEAR("sameWorkPlaceAndYear");


    private final String configName;

    FixedCategory(String configName) {
        this.configName = configName;
    }

    public String getConfigName() {
        return configName;
    }
}
