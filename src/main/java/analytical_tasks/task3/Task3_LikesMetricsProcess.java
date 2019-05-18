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

public class Task3_LikesMetricsProcess extends KeyedBroadcastProcessFunction<String, LikeEvent, PostEvent, Tuple4<String, Integer, Double, Long>> {

    private transient HashMap<String, String> postBelongsTo;

    private transient ValueState<Integer> count;
    private transient ListState<LikeEvent> likeEventsBuffer;
    private transient ValueState<Long> processedWatermark;
    private transient ValueState<HashSet<String>> personLikesPerson;

    // Initialize Logger
    private static Logger logger = LoggerFactory.getLogger(Task3_LikesMetricsProcess.class);

    @Override
    public void processElement(LikeEvent likeEvent, ReadOnlyContext readOnlyContext,
                               Collector<Tuple4<String, Integer, Double, Long>> collector) throws Exception {

        count.update(count.value() + 1);
        likeEventsBuffer.add(likeEvent);

        if (processedWatermark.value() < readOnlyContext.timerService().currentWatermark()) {

            processedWatermark.update(readOnlyContext.timerService().currentWatermark());
            ArrayList<LikeEvent> notToBeProcessedItems = new ArrayList<>();
            HashSet<String> currentPersonLikesPerson = personLikesPerson.value();

            for (LikeEvent event : likeEventsBuffer.get()) {

                if (event.getTimeMilisecond() < processedWatermark.value()) {

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

            collector.collect(new Tuple4<>(likeEvent.getPersonId(),
                    5,
                    (double) currentPersonLikesPerson.size() / count.value()
                    , likeEvent.getTimestamp()));
        }

    }

    @Override
    public void processBroadcastElement(PostEvent postEvent, Context context,
                                        Collector<Tuple4<String, Integer, Double, Long>> collector) throws Exception {
        postBelongsTo.put(postEvent.getId(), postEvent.getPersonId());
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


        ValueStateDescriptor<Long> processedWatermarkDescriptor =
                new ValueStateDescriptor<>("processedWatermark",
                        BasicTypeInfo.LONG_TYPE_INFO, 0L);

        processedWatermark = getRuntimeContext().getState(processedWatermarkDescriptor);

        ValueStateDescriptor<HashSet<String>> personLikesPersonDescriptor =
                new ValueStateDescriptor<>(
                        "personLikesPerson",
                        TypeInformation.of(new TypeHint<HashSet<String>>() {
                        }),
                        new HashSet<>());

        personLikesPerson = getRuntimeContext().getState(personLikesPersonDescriptor);

    }
}
