package analytical_tasks.task3;

import model.CommentEvent;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import static analytical_tasks.task3.Task3_TextMetrics.*;

public class Task3_CommentsMetricsProcess extends KeyedProcessFunction<String, CommentEvent, Tuple4<String, Integer, Double, Long>> {

    /**
     * A counter which is used to calculate average number of unique words over all words in a Person's Comments
     */
    private transient ValueState<Double> calculateUniqueWordsOverWordsMetricSum;

    /**
     * Average number of bad words in a Person's Comments
     */
    private transient ValueState<Double> profanityFilteredWords;

    /**
     * Counter for each word used by a Person
      */
    private transient MapState<String, Integer> numberOfWordsUsed;

    /**
     * Number of comments processed comments
     */
    private transient ValueState<Integer> count;

    /**
     * A flag which is used to understand whether the first timer is set. Since onTimer() method registers the next timer
     * only setting the first timer is enough in processElement.
     */
    private transient ValueState<Boolean> timerSet;

    @Override
    public void processElement(CommentEvent commentEvent, Context context,
                               Collector<Tuple4<String, Integer, Double, Long>> collector) throws Exception {

        if (!timerSet.value()) {
            context.timerService().registerEventTimeTimer(context.timerService().currentWatermark() + 1);
            timerSet.update(true);
        }

        String comment = commentEvent.getContent();

        Double uniqueOverWord = calculateUniqueWordsOverWordsMetric(comment);
        calculateUniqueWordsOverWordsMetricSum.update(calculateUniqueWordsOverWordsMetricSum.value() + uniqueOverWord);

        // Count distinct words in the comment
        numberOfWordsUsed.putAll(vectorize(comment));

        // Increment the number of total posts
        count.update(count.value() + 1);

        // Profanity filtering
        profanityFilteredWords.update(profanityFilteredWords.value() + countBadWords(comment));

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple4<String, Integer, Double, Long>> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        ctx.timerService().registerEventTimeTimer(ctx.timerService().currentWatermark() + 1);

        out.collect(new Tuple4<>(ctx.getCurrentKey(), 2, calculateUniqueWordsOverWordsMetricSum.value() / count.value(), timestamp));
        out.collect(new Tuple4<>(ctx.getCurrentKey(), 3, profanityFilteredWords.value() / count.value(), timestamp));

        double distinctWords = 0.0;
        for (String key : numberOfWordsUsed.keys()) {
            distinctWords++;
        }

        out.collect(new Tuple4<>(ctx.getCurrentKey(), 4, distinctWords / count.value(), timestamp));
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // All words used by a person and their counts
        MapStateDescriptor<String, Integer> numberOfWordsUsedDescriptor =
                new MapStateDescriptor<>(
                        "numberOfWordsUsed",
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO);

        numberOfWordsUsed = getRuntimeContext().getMapState(numberOfWordsUsedDescriptor);

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

        profanityFilteredWords =
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
