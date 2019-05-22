package analytical_tasks.task2;

/**
 * This enum represents the different Friend features
 * that are used for the friend suggestions. The field
 * configName is used to have a faster access to the
 * properties file.
 */
public enum FixedCategory {
    SAME_FORUM_MEMBER("sameForumMember"),
    SAME_FORUM_MODERATOR("sameForumModerator"),
    SAME_FORUM_MEMBER_MODERATOR("sameForumMemberModerator"),
    SAME_AGE("sameAge"),
    SAME_BROWSER("sameBrowser"),
    SAME_INTEREST_TAG("sameInterestTag"),
    SAME_LANGUAGE("sameLanguage"),
    LIKED_THE_SAME("likedTheSame"),
    SAME_FORUM_POST("postedSameForum"),
    COMMENTED_SAME_POST("commentedSamePost"),
    COMMENTED_SAME_COMMENT("commentedSameComment"),
    ACTIVE("active");


    private final String configName;

    FixedCategory(String configName) {
        this.configName = configName;
    }

    public String getConfigName() {
        return configName;
    }
}
