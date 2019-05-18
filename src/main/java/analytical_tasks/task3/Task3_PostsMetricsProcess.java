package analytical_tasks.task3;

import model.PostEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import static analytical_tasks.task3.Task3_Metrics.calculateUniqueWordsOverWordsMetric;
import static analytical_tasks.task3.Task3_Metrics.countBadWords;

public class Task3_PostsMetricsProcess extends KeyedProcessFunction<String, PostEvent, Tuple4<String, Integer, Double, Long>> {

        private transient ValueState<Long> processedWatermark;
        private transient ValueState<Double> calculateUniqueWordsOverWordsMetricSum;
        private transient ValueState<Double> profanityFilteredWords;
        private transient ValueState<Integer> count;

        @Override
        public void processElement(PostEvent postEvent, Context context,
                Collector<Tuple4<String, Integer, Double, Long>> collector) throws Exception {

            String post = postEvent.getContent();

            Double uniqueOverWord = calculateUniqueWordsOverWordsMetric(post);
            calculateUniqueWordsOverWordsMetricSum.update(
                    calculateUniqueWordsOverWordsMetricSum.value() +
                            uniqueOverWord);

            // Increment the number of total posts
            count.update(count.value() + 1);

            // Profanity filtering
            profanityFilteredWords.update(profanityFilteredWords.value() + countBadWords(post));


            if (processedWatermark.value() < context.timerService().currentWatermark()) {

                processedWatermark.update(context.timerService().currentWatermark());

                collector.collect(new Tuple4<>(postEvent.getPersonId(),
                        0,
                        calculateUniqueWordsOverWordsMetricSum.value()/count.value(),
                        postEvent.getTimeMilisecond()));

                collector.collect(new Tuple4<>(postEvent.getPersonId(),
                        1,
                        profanityFilteredWords.value()/count.value(),
                        postEvent.getTimeMilisecond()));

            }

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

