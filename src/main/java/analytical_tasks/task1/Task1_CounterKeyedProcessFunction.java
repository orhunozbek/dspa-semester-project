package analytical_tasks.task1;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Task1_CounterKeyedProcessFunction extends KeyedProcessFunction<Tuple, Tuple4<String, Integer, String, Long>, Tuple4<Long, String, String, Integer>> {

    // For determining whether a new user likes/replies the post
    private transient MapState<String, Integer> userEngagement;

    // For determining whether the post is active or not
    private transient ValueState<Long> latestEvent;
    private transient ValueState<Boolean> timerSet;

    // Counters for 3 statistics that we are interested in
    private transient ValueState<Integer> userEngagementCounter;
    private transient ValueState<Integer> commentCounter;
    private transient ValueState<Integer> replyCounter;


    @Override
    public void processElement(Tuple4<String, Integer, String, Long> t,
                               Context context, Collector<Tuple4<Long, String, String, Integer>> collector) throws Exception {

        if (t.f3 > latestEvent.value()) latestEvent.update(t.f3);
        if (!timerSet.value()) {
            // Get the window boundary right after the event
            context.timerService().registerEventTimeTimer(t.f3 - t.f3 % 1800000 + 1800000);
            timerSet.update(true);
        }

        // If this is a comment increment both comment counter and update counter
        if (t.f1 == 1) {
            commentCounter.update(commentCounter.value() + 1);
            replyCounter.update(replyCounter.value() + 1);
        } else if (t.f1 == 0) {
            replyCounter.update(replyCounter.value() + 1);
        }

        if (!userEngagement.contains(t.f2)) {
            userEngagement.put(t.f2, 1);
            userEngagementCounter.update(userEngagementCounter.value() + 1);
        }

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple4<Long, String, String, Integer>> out) throws Exception {
        super.onTimer(timestamp, ctx, out);

        // Set the next timer
        ctx.timerService().registerEventTimeTimer(timestamp + 1800000);

        // Check if the post is active
        if (latestEvent.value() > timestamp - 12 * 60 * 60 * 1000) {

            // Get key as a string
            String keyString = ctx.getCurrentKey().toString();
            keyString = keyString.substring(1, keyString.length() - 1);

            // Timestamp, PostID, EventType, Count --> Output
            out.collect(new Tuple4<>(timestamp, keyString, "comment", commentCounter.value()));
            out.collect(new Tuple4<>(timestamp, keyString, "reply", replyCounter.value()));

            if (timestamp % 3600000 == 0) {
                out.collect(new Tuple4<>(timestamp, keyString, "userEngagement", userEngagementCounter.value()));
            }
        }

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        ValueStateDescriptor<Long> latestEventDescriptor =
                new ValueStateDescriptor<>("latestEvent",
                        BasicTypeInfo.LONG_TYPE_INFO, 0L);

        latestEvent = getRuntimeContext().getState(latestEventDescriptor);

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
    }
}
