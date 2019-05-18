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

import static analytical_tasks.task3.Task3_Metrics.*;

public class Task3_CommentsMetricsProcess extends KeyedProcessFunction<String, CommentEvent, Tuple4<String, Integer, Double, Long>> {

    private transient ValueState<Double> calculateUniqueWordsOverWordsMetricSum;
    private transient ValueState<Double> profanityFilteredWords;
    private transient MapState<String, Integer> numberOfWordsUsed;
    private transient ValueState<Integer> count;

    private transient ValueState<Long> processedWatermark;

    @Override
    public void processElement(CommentEvent commentEvent, Context context,
                               Collector<Tuple4<String, Integer, Double, Long>> collector) throws Exception {

        String comment = commentEvent.getContent();

        Double uniqueOverWord = calculateUniqueWordsOverWordsMetric(comment);
        calculateUniqueWordsOverWordsMetricSum.update(
                calculateUniqueWordsOverWordsMetricSum.value() +
                        uniqueOverWord);

        // Count distinct words in the comment
        numberOfWordsUsed.putAll(vectorize(comment));

        // Increment the number of total posts
        count.update(count.value() + 1);

        // Profanity filtering
        profanityFilteredWords.update(profanityFilteredWords.value() + countBadWords(comment));

        if (processedWatermark.value() < context.timerService().currentWatermark()) {

            processedWatermark.update(context.timerService().currentWatermark());

            collector.collect(new Tuple4<>(commentEvent.getPersonId(),
                    2,
                    calculateUniqueWordsOverWordsMetricSum.value() / count.value(),
                    commentEvent.getTimeMilisecond()));

            collector.collect(new Tuple4<>(commentEvent.getPersonId(),
                    3,
                    profanityFilteredWords.value() / count.value(),
                    commentEvent.getTimeMilisecond()));

            double distinctWords = 0.0;
            for (String key : numberOfWordsUsed.keys()) {
                distinctWords++;
            }

            collector.collect(new Tuple4<>(commentEvent.getPersonId(),
                    4,
                    distinctWords / count.value(),
                    commentEvent.getTimeMilisecond()));
        }
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

        ValueStateDescriptor<Long> processedWatermarkDescriptor =
                new ValueStateDescriptor<>("processedWatermark",
                        BasicTypeInfo.LONG_TYPE_INFO, 0L);

        processedWatermark = getRuntimeContext().getState(processedWatermarkDescriptor);
    }

}
