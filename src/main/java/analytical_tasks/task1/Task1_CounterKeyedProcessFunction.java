package analytical_tasks.task1;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 *
 */
public class Task1_CounterKeyedProcessFunction extends KeyedProcessFunction<Tuple, Tuple4<String, Integer, String, Long>, Tuple4<Long, String, String, Integer>> {

    /**
     * A set which contains the personID's for all Persons engaged by the post. Used to decide whether a user engagement
     * event is unique or not.
     */
    private transient MapState<String, Integer> userEngagement;

    /**
     * A timestamp which represents the timestamp of the latest event (Event Time). Used t
     */
    private transient ValueState<Long> latestEvent;


    private transient ValueState<Boolean> timerSet;

    // The timestamp of the current window
    private transient ValueState<Long> currentWindowBegin;

    // Counters for 3 statistics that we are interested in
    private transient ValueState<Integer> userEngagementCounter;
    private transient ValueState<Integer> commentCounter;
    private transient ValueState<Integer> replyCounter;

    // Queues for events which come early
    private transient ListState<Long> replyQueue;
    private transient ListState<Long> commentQueue;
    private transient ListState<Tuple2<Long, String>> userEngagementQueue;


    // Constants for eviction of events
    public static final long replyUpdateTime = 1000 * 60 * 30;
    public static final long userEngagementUpdateTime = 1000 * 60 * 60;
    private static final long activeUserThreshold = 12 * 60 * 60 * 1000;

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
        if (t.f1 == 1) {

            if (t.f3 < currentWindowBegin.value() + replyUpdateTime) {
                commentCounter.update(commentCounter.value() + 1);
                replyCounter.update(replyCounter.value() + 1);
            } else {
                commentQueue.add(t.f3);
                replyQueue.add(t.f3);
            }


        } else if (t.f1 == 0) {

            if (t.f3 < currentWindowBegin.value() + replyUpdateTime) {
                replyCounter.update(replyCounter.value() + 1);
            } else {
                replyQueue.add(t.f3);
            }
        }

        if (!userEngagement.contains(t.f2)) {
            if (t.f3 < currentWindowBegin.value() + userEngagementUpdateTime) {
                userEngagement.put(t.f2, 1);
                userEngagementCounter.update(userEngagementCounter.value() + 1);
            } else {
                userEngagementQueue.add(new Tuple2<>(t.f3, t.f2));
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

        ArrayList<Long> newReplyQueue = new ArrayList<>();
        for (long queuedReply : replyQueue.get()) {
            if (queuedReply <= currentWindowBegin.value() + replyUpdateTime) {
                replyCounter.update(replyCounter.value() + 1);
            } else {
                newReplyQueue.add(queuedReply);
            }
        }
        replyQueue.update(newReplyQueue);

        ArrayList<Long> newCommentQueue = new ArrayList<>();
        for (long queuedComment : commentQueue.get()) {
            if (queuedComment <= currentWindowBegin.value() + replyUpdateTime) {
                commentCounter.update(commentCounter.value() + 1);
            } else {
                newCommentQueue.add(queuedComment);
            }
        }
        commentQueue.update(newCommentQueue);

        ArrayList<Tuple2<Long, String>> newEngagementQueue = new ArrayList<>();

        for (Tuple2<Long, String> queuedEngagement : userEngagementQueue.get()) {
            if (queuedEngagement.f0 <= currentWindowBegin.value() + userEngagementUpdateTime) {
                if (!userEngagement.contains(queuedEngagement.f1)) {
                    userEngagement.put(queuedEngagement.f1, 1);
                    userEngagementCounter.update(userEngagementCounter.value() + 1);
                }
            } else {
                newEngagementQueue.add(queuedEngagement);
            }
        }
        userEngagementQueue.update(newEngagementQueue);

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

        ListStateDescriptor<Long> replyQueueDescriptor = new ListStateDescriptor<Long>(
                "replyQueue",
                BasicTypeInfo.LONG_TYPE_INFO
        );
        replyQueue = getRuntimeContext().getListState(replyQueueDescriptor);

        ListStateDescriptor<Long> commentQueueDescriptor = new ListStateDescriptor<Long>(
                "commentQueue",
                BasicTypeInfo.LONG_TYPE_INFO
        );
        commentQueue = getRuntimeContext().getListState(commentQueueDescriptor);

        ListStateDescriptor<Tuple2<Long, String>> userEngagementQueueDescriptor = new ListStateDescriptor<>(
                "userEngagementQueue",
                TypeInformation.of(new TypeHint<Tuple2<Long, String>>() {
                })
        );

        userEngagementQueue = getRuntimeContext().getListState(userEngagementQueueDescriptor);

    }
}
