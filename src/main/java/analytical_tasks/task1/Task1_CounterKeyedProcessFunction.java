package analytical_tasks.task1;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.ArrayList;

/**
 * A KeyedProcessFunction which counts comments, replies and unique user engagement from the beginning of the simulation.
 * The Process is partitioned by postID. All keys emit events at the same event time; for each 30 minutes. Replies
 * and Comments are updated each 30 minutes and user engagement statistics are updated each hour. If an event comes
 * earlier than its EventTime, meaning that its EventTime is after the current window, the starting time of its
 * window is calculated, and then a cache entry which maps that window to the count of events that happened at that
 * window is created. These cached event counts are added to the sum once their window starts. Results are emitted for
 * all active posts
 *
 *
 * Author: oezbeko
 */
public class Task1_CounterKeyedProcessFunction extends KeyedProcessFunction<Tuple, Tuple4<String, Integer, String, Long>,
        Tuple4<Long, String, String, Integer>> {

    /**
     * A set which contains the personID's for all Persons engaged by the post. Used to decide whether a user engagement
     * event is unique or not.
     */
    private transient MapState<String, Integer> userEngagement;

    /**
     * A timestamp which represents the timestamp of the latest event (Event Time). Used t
     */
    private transient ValueState<Long> latestEvent;

    /**
     * A flag which is used to understand whether the first timer is set. Since onTimer() method registers the next timer
     * only setting the first timer is enough in processElement.
     */
    private transient ValueState<Boolean> timerSet;

    /**
     * The beginning timestamp of the current window in EventTime. It is updated onTimer() method and set for the first
     * time in processElement function.
     */
    private transient ValueState<Long> currentWindowBegin;

    /**
     * Counters for the statistics that we are interested in. They represent the counts until the end of the window
     * starting from currentWindowBegin
     */
    private transient ValueState<Integer> userEngagementCounter;
    private transient ValueState<Integer> commentCounter;
    private transient ValueState<Integer> replyCounter;

    /**
     * A map which projects future window beginnings to count of events within that timewindow. Once current watermark
     * passes the start of a window which is stored in these counters, these counts are added to the main counters.
     */
    private transient MapState<Long, Integer> replyCache;
    private transient MapState<Long, Integer> commentCache;
    private transient MapState<Long, ArrayList<String>> userEngagementCache;


    /***
     * Constants for update frequencies and active post definitions.
     */
    public static final long replyUpdateTime = 1000 * 60 * 30;
    public static final long userEngagementUpdateTime = 1000 * 60 * 60;
    private static final long activeUserThreshold = 12 * 60 * 60 * 1000;

    /**
     * Returns the nearest window beginning before the timestamp
     *
     * @param timestamp Event timestamp
     * @param updateTime updateTime, since we need to maintain windows with different sizes for different counters
     * @return Nearest window beginning before the timestamp
     */
    private long getNearestWindowTime (long timestamp, long updateTime) throws IOException {
        long startingTime = currentWindowBegin.value();

        while (timestamp >= (startingTime + updateTime)){
            startingTime += updateTime;
        }

        return startingTime;
    }

    @Override
    public void processElement(Tuple4<String, Integer, String, Long> t,
                               Context context, Collector<Tuple4<Long, String, String, Integer>> collector) throws Exception {

        if (t.f3 > latestEvent.value()) latestEvent.update(t.f3);
        if (!timerSet.value()) {
            // Get the window boundary right after the event
            context.timerService().registerEventTimeTimer(t.f3 - t.f3 % replyUpdateTime + replyUpdateTime);
            currentWindowBegin.update(t.f3 - t.f3 % replyUpdateTime);
            timerSet.update(true);
        }

        // If this is a comment increment both comment counter and reply counter
        // If the event belongs to current timestamp, directly add it to the counter, otherwise cache the count

        if (t.f1 == 1) {

            if (t.f3 < currentWindowBegin.value() + replyUpdateTime) {
                commentCounter.update(commentCounter.value() + 1);
                replyCounter.update(replyCounter.value() + 1);
            } else {
                long eventWindowTime = getNearestWindowTime(t.f3, replyUpdateTime);

                if (commentCache.contains(eventWindowTime)) commentCache.put(eventWindowTime, commentCache.get(eventWindowTime) + 1);
                else commentCache.put(eventWindowTime,1);

                if (replyCache.contains(eventWindowTime)) replyCache.put(eventWindowTime, replyCache.get(eventWindowTime) + 1);
                else replyCache.put(eventWindowTime,1);
            }


        } else if (t.f1 == 0) {

            if (t.f3 < currentWindowBegin.value() + replyUpdateTime) {
                replyCounter.update(replyCounter.value() + 1);
            } else {
                long eventWindowTime = getNearestWindowTime(t.f3, replyUpdateTime);

                if (replyCache.contains(eventWindowTime)) replyCache.put(eventWindowTime, replyCache.get(eventWindowTime) + 1);
                else replyCache.put(eventWindowTime,1);
            }
        }

        // IMPORTANT REMARK: Here the logic is slightly different, we also need to cache the userID since maybe the event
        // could be unique in ProcessingTime but not unique in EventTime; because there is another event with the same
        // personID processed after but happened earlier in EventTime

        if (!userEngagement.contains(t.f2)) {
            if (t.f3 < currentWindowBegin.value() + userEngagementUpdateTime) {
                userEngagement.put(t.f2, 1);
                userEngagementCounter.update(userEngagementCounter.value() + 1);
            } else {
                long eventWindowTime = getNearestWindowTime(t.f3, userEngagementUpdateTime);

                if (userEngagementCache.contains(eventWindowTime)){
                    ArrayList<String> cur = userEngagementCache.get(eventWindowTime);
                    cur.add(t.f2);
                    userEngagementCache.put(eventWindowTime,cur);
                } else{
                    ArrayList<String> cur = new ArrayList<>();
                    cur.add (t.f2);
                    userEngagementCache.put(eventWindowTime,cur);
                }
            }
        }

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple4<Long, String, String, Integer>> out) throws Exception {
        super.onTimer(timestamp, ctx, out);

        // Set the next timer
        ctx.timerService().registerEventTimeTimer(timestamp + replyUpdateTime);
        currentWindowBegin.update(timestamp);

        // Check if the post is active
        if (latestEvent.value() > timestamp - activeUserThreshold) {

            // Get key as a string
            String keyString = ctx.getCurrentKey().toString();
            keyString = keyString.substring(1, keyString.length() - 1);

            // Timestamp, PostID, EventType, Count --> Output
            // Collect counts as events for each key separately
            out.collect(new Tuple4<>(timestamp, keyString, "comment", commentCounter.value()));
            out.collect(new Tuple4<>(timestamp, keyString, "reply", replyCounter.value()));
            out.collect(new Tuple4<>(timestamp, keyString, "userEngagement", userEngagementCounter.value()));

        }

        // Update counters if there are events which come earlier and belongs to the window beginning with
        // currentWindowBegin

        if (commentCache.contains(currentWindowBegin.value())) {
            commentCounter.update(commentCounter.value() + commentCache.get(currentWindowBegin.value()));
            commentCache.remove(currentWindowBegin.value());
        }

        if (replyCache.contains(currentWindowBegin.value())) {
            replyCounter.update(replyCounter.value() + replyCache.get(currentWindowBegin.value()));
            replyCache.remove(currentWindowBegin.value());
        }

        if (userEngagementCache.contains(currentWindowBegin.value())){

            Iterable<String> possibleUserEngagementEvents = userEngagementCache.get(currentWindowBegin.value());

            // If the event is unique, meaning that it is the first engagement received from the user, then update the count.
            for (String personID: possibleUserEngagementEvents){
                if (!userEngagement.contains(personID)){
                    userEngagement.put(personID,1);
                    userEngagementCounter.update(userEngagementCounter.value() + 1);
                }
            }
            userEngagementCache.remove(currentWindowBegin.value());
        }

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        ValueStateDescriptor<Long> latestEventDescriptor =
                new ValueStateDescriptor<>("latestEvent",
                        BasicTypeInfo.LONG_TYPE_INFO, 0L);

        latestEvent = getRuntimeContext().getState(latestEventDescriptor);

        ValueStateDescriptor<Long> currentWindowBeginDescriptor =
                new ValueStateDescriptor<>("currentWindowBegin",
                        BasicTypeInfo.LONG_TYPE_INFO, 0L);

        currentWindowBegin = getRuntimeContext().getState(currentWindowBeginDescriptor);

        ValueStateDescriptor<Boolean> timerSetDescriptor =
                new ValueStateDescriptor<>("timerSet",
                        BasicTypeInfo.BOOLEAN_TYPE_INFO, false);

        timerSet = getRuntimeContext().getState(timerSetDescriptor);

        ValueStateDescriptor<Integer> userEngagementCounterDescriptor =
                new ValueStateDescriptor<>("userEngagementCounter",
                        BasicTypeInfo.INT_TYPE_INFO, 0);

        userEngagementCounter = getRuntimeContext().getState(userEngagementCounterDescriptor);

        ValueStateDescriptor<Integer> commentCounterDescriptor =
                new ValueStateDescriptor<>("commentCounter",
                        BasicTypeInfo.INT_TYPE_INFO, 0);

        commentCounter = getRuntimeContext().getState(commentCounterDescriptor);


        ValueStateDescriptor<Integer> replyCounterDescriptor =
                new ValueStateDescriptor<>("replyCounter",
                        BasicTypeInfo.INT_TYPE_INFO, 0);

        replyCounter = getRuntimeContext().getState(replyCounterDescriptor);

        MapStateDescriptor<String, Integer> userEngagementDescriptor =
                new MapStateDescriptor<>(
                        "userEngagement",
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO);

        userEngagement = getRuntimeContext().getMapState(userEngagementDescriptor);

        MapStateDescriptor<Long, Integer> replyCacheDescriptor = new MapStateDescriptor<>(
                "replyCache",
                BasicTypeInfo.LONG_TYPE_INFO,
                BasicTypeInfo.INT_TYPE_INFO
        );
        replyCache = getRuntimeContext().getMapState(replyCacheDescriptor);

        MapStateDescriptor<Long, Integer> commentCacheDescriptor = new MapStateDescriptor<>(
                "commentCache",
                BasicTypeInfo.LONG_TYPE_INFO,
                BasicTypeInfo.INT_TYPE_INFO
        );
        commentCache = getRuntimeContext().getMapState(commentCacheDescriptor);

        MapStateDescriptor<Long, ArrayList<String>> userEngagementCacheDescriptor = new MapStateDescriptor<Long, ArrayList<String>>(
                "userEngagementCache",
                BasicTypeInfo.LONG_TYPE_INFO,
                TypeInformation.of(new TypeHint<ArrayList<String>>() {})
        );

        userEngagementCache = getRuntimeContext().getMapState(userEngagementCacheDescriptor);

    }
}
