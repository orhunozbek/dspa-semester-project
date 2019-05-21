package model;

import org.jetbrains.annotations.NotNull;

public abstract class Event implements Comparable<Event>{

    abstract public String getTopicName();
    abstract public long getTimestamp();

    @Override
    public int compareTo(@NotNull Event o) {
        return (int)(this.getTimestamp() - o.getTimestamp());
    }
}
