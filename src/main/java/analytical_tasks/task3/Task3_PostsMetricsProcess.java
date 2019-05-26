package analytical_tasks.task3;

import model.PostEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import static analytical_tasks.task3.Task3_TextMetrics.calculateUniqueWordsOverWordsMetric;
import static analytical_tasks.task3.Task3_TextMetrics.countBadWords;

/**
 * A KeyedProcessFunction for the calculation of metrics about Posts. The Process is Keyed by PersonID. Two types of
 * features are calculated.
 *
 * 1. Number of Unique Words in a sentence compared to total number of words in a Post. If this metric is low,
 * it indicates that a Person uses same words repetitively
 * 2. Number of profanityFilteredWords used by the person compared to number of posts
 *
 *
 * Author: oezbeko
 */
public class Task3_PostsMetricsProcess extends KeyedProcessFunction<String, PostEvent, Tuple4<String, Integer, Double, Long>> {

    /**
     * A counter which is used to calculate average number of unique words over all words in a Person's Posts
     */
    private transient ValueState<Double> calculateUniqueWordsOverWordsMetricSum;

    /**
     * Average number of bad words in a Person's Posts
     */
    private transient ValueState<Double> numberOfProfanityFilteredWords;
    private transient ValueState<Integer> count;

    /**
     * A flag which is used to understand whether the first timer is set. Since onTimer() method registers the next timer
     * only setting the first timer is enough in processElement.
     */
    private transient ValueState<Boolean> timerSet;

    @Override
    public void processElement(PostEvent postEvent, Context context,
                               Collector<Tuple4<String, Integer, Double, Long>> collector) throws Exception {

        if (!timerSet.value()) {
            context.timerService().registerEventTimeTimer(context.timerService().currentWatermark() + 1);
            timerSet.update(true);
        }

        String post = postEvent.getContent();

        Double uniqueOverWord = calculateUniqueWordsOverWordsMetric(post);
        calculateUniqueWordsOverWordsMetricSum.update(calculateUniqueWordsOverWordsMetricSum.value() + uniqueOverWord);

        // Increment the number of total posts
        count.update(count.value() + 1);

        // Profanity filtering
        numberOfProfanityFilteredWords.update(numberOfProfanityFilteredWords.value() + countBadWords(post));

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx,
                        Collector<Tuple4<String, Integer, Double, Long>> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        ctx.timerService().registerEventTimeTimer(ctx.timerService().currentWatermark() + 1);
        out.collect(new Tuple4<>(ctx.getCurrentKey(), 0, calculateUniqueWordsOverWordsMetricSum.value() / count.value(), timestamp));
        out.collect(new Tuple4<>(ctx.getCurrentKey(), 1, numberOfProfanityFilteredWords.value() / count.value(), timestamp));
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // Feature, number of unique words over words in a Comment, Text feature
        ValueStateDescriptor<Double> calculateUniqueWordsOverWordsMetricSumDescriptor =
                new ValueStateDescriptor<>("calculateUniqueWordsOverWordsMetricSum",
                        BasicTypeInfo.DOUBLE_TYPE_INFO, 0.0);

        calculateUniqueWordsOverWordsMetricSum =
                getRuntimeContext().getState(calculateUniqueWordsOverWordsMetricSumDescriptor);

        // Number of messages which did not pass the profanity filter
        ValueStateDescriptor<Double> profanityFilteredWordsDescriptor =
                new ValueStateDescriptor<>("profanityFilteredMessages",
                        BasicTypeInfo.DOUBLE_TYPE_INFO, 0.0);

        numberOfProfanityFilteredWords =
                getRuntimeContext().getState(profanityFilteredWordsDescriptor);

        // State for counter of CommentEvents for each PersonID
        ValueStateDescriptor<Integer> countDescriptor =
                new ValueStateDescriptor<>("count",
                        BasicTypeInfo.INT_TYPE_INFO, 0);

        count = getRuntimeContext().getState(countDescriptor);

        ValueStateDescriptor<Boolean> timerSetDescriptor =
                new ValueStateDescriptor<>("timerSet",
                        BasicTypeInfo.BOOLEAN_TYPE_INFO, false);

        timerSet = getRuntimeContext().getState(timerSetDescriptor);

    }

}

