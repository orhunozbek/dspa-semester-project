package analytical_tasks.task3;

import model.LikeEvent;
import model.PostEvent;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;


/**
 * A KeyedProcessFunction for the calculation of metrics about Likes. The Process is Keyed by PersonID. One feature is
 * calculated
 *
 * 1. Number of unique people liked by a person compared to number of likes by the user
 *
 * Author: oezbeko
 */
public class Task3_LikesMetricsProcess extends KeyedBroadcastProcessFunction<String, LikeEvent, PostEvent, Tuple4<String, Integer, Double, Long>> {

    // Initialize Logger
    private static Logger logger = LoggerFactory.getLogger(Task3_LikesMetricsProcess.class);

    private transient HashMap<String, String> postBelongsTo;

    private transient ValueState<Integer> count;
    private transient ListState<LikeEvent> likeEventsBuffer;
    private transient ValueState<HashSet<String>> personLikesPerson;


    /**
     * A flag which is used to understand whether the first timer is set. Since onTimer() method registers the next timer
     * only setting the first timer is enough in processElement.
     */
    private transient ValueState<Boolean> timerSet;


    @Override
    public void processElement(LikeEvent likeEvent, ReadOnlyContext readOnlyContext,
                               Collector<Tuple4<String, Integer, Double, Long>> collector) throws Exception {

        if (!timerSet.value()) {
            readOnlyContext.timerService().registerEventTimeTimer(readOnlyContext.timerService().currentWatermark() + 1);
            timerSet.update(true);
        }

        count.update(count.value() + 1);
        likeEventsBuffer.add(likeEvent);

    }

    @Override
    public void processBroadcastElement(PostEvent postEvent, Context context,
                                        Collector<Tuple4<String, Integer, Double, Long>> collector) throws Exception {
        postBelongsTo.put(postEvent.getId(), postEvent.getPersonId());
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple4<String, Integer, Double, Long>> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        ctx.timerService().registerEventTimeTimer(ctx.timerService().currentWatermark() + 1);

        ArrayList<LikeEvent> notToBeProcessedItems = new ArrayList<>();
        HashSet<String> currentPersonLikesPerson = personLikesPerson.value();

        for (LikeEvent event : likeEventsBuffer.get()) {

            if (event.getTimeMilisecond() < timestamp) {

                if (postBelongsTo.containsKey(event.getPostId())) {

                    String personID = postBelongsTo.get(event.getPostId());
                    currentPersonLikesPerson.add(personID);

                } else {
                    logger.warn("LikeEvent with postID:{} could not be matched.", event.getPostId());
                    notToBeProcessedItems.add(event);
                }

            } else {
                notToBeProcessedItems.add(event);
            }
        }

        personLikesPerson.update(currentPersonLikesPerson);
        likeEventsBuffer.clear();
        likeEventsBuffer.addAll(notToBeProcessedItems);

        out.collect(new Tuple4<>(ctx.getCurrentKey(), 5, (double) currentPersonLikesPerson.size() / count.value(), timestamp));

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // All words used by a person and their counts
        postBelongsTo = new HashMap<>();

        ValueStateDescriptor<Integer> countDescriptor =
                new ValueStateDescriptor<>("count",
                        BasicTypeInfo.INT_TYPE_INFO, 0);

        count = getRuntimeContext().getState(countDescriptor);


        ListStateDescriptor<LikeEvent> likeEventsBufferDescriptor = new ListStateDescriptor<LikeEvent>(
                "likeEventsBuffer",
                TypeInformation.of(LikeEvent.class)
        );

        likeEventsBuffer = getRuntimeContext().getListState(likeEventsBufferDescriptor);


        ValueStateDescriptor<HashSet<String>> personLikesPersonDescriptor =
                new ValueStateDescriptor<>(
                        "personLikesPerson",
                        TypeInformation.of(new TypeHint<HashSet<String>>() {
                        }),
                        new HashSet<>());

        personLikesPerson = getRuntimeContext().getState(personLikesPersonDescriptor);

        ValueStateDescriptor<Boolean> timerSetDescriptor =
                new ValueStateDescriptor<>("timerSet",
                        BasicTypeInfo.BOOLEAN_TYPE_INFO, false);

        timerSet = getRuntimeContext().getState(timerSetDescriptor);

    }
}
