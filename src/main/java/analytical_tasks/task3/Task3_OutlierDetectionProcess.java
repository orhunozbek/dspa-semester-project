package analytical_tasks.task3;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class Task3_OutlierDetectionProcess extends KeyedProcessFunction<Tuple, Tuple4<String, Integer, Double, Long>,
        Tuple5<String, Integer, Double, Double, Long>> {

    private transient ValueState<Double> average;
    private transient ValueState<Double> stdev;
    private transient ValueState<Long> statsCalculationTime;
    private transient MapState<String, Double> currentValues;

    // Time to wait in order to update mean and stdev
    private static final long waitTime = 24 * 1000 * 60 * 60;
    private static Logger logger = LoggerFactory.getLogger(Task3_OutlierDetectionProcess.class);

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // State for average
        ValueStateDescriptor<Double> averageDescriptor =
                new ValueStateDescriptor<>("average", BasicTypeInfo.DOUBLE_TYPE_INFO, 0.0);

        average = getRuntimeContext().getState(averageDescriptor);

        // State for stdev
        ValueStateDescriptor<Double> stdevDescriptor =
                new ValueStateDescriptor<>("stdev", BasicTypeInfo.DOUBLE_TYPE_INFO, 0.0);

        stdev = getRuntimeContext().getState(stdevDescriptor);

        // State for MapState
        MapStateDescriptor<String, Double> currentValuesDescriptor =
                new MapStateDescriptor<>(
                        "currentValues",
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.DOUBLE_TYPE_INFO);

        currentValues = getRuntimeContext().getMapState(currentValuesDescriptor);

        ValueStateDescriptor<Long> statsCalculatedDescriptor =
                new ValueStateDescriptor<>("statsCalculated",
                        BasicTypeInfo.LONG_TYPE_INFO, 0L);

        statsCalculationTime = getRuntimeContext().getState(statsCalculatedDescriptor);

    }

    @Override
    public void processElement(Tuple4<String, Integer, Double, Long> t,
                               Context context,
                               Collector<Tuple5<String, Integer, Double, Double, Long>> collector) throws Exception {

        // Update the state with latest information
        currentValues.put(t.f0, t.f2);

        if (statsCalculationTime.value() + waitTime < context.timerService().currentWatermark()) {

            statsCalculationTime.update(context.timerService().currentWatermark());

            Double sum = 0.0;
            double varianceAccumulator = 0.0;
            int count = 0;


            for (Map.Entry<String, Double> entry : currentValues.entries()) {
                sum += entry.getValue();
                count++;
            }

            average.update(sum / count);

            for (Map.Entry<String, Double> entry : currentValues.entries()) {
                varianceAccumulator += Math.pow(average.value() - entry.getValue(), 2);
            }

            stdev.update(Math.sqrt(varianceAccumulator / count));
            logger.info("Mean and standard deviation for feature {} is calculated. Mean: {}, StDev: {}",
                    t.f1, average.value(), stdev.value());
        }

        // If mean and variance is calculated, we can run
        if (statsCalculationTime.value() != 0L) {

            // If the new value is more than 2 stdevs away from the mean
            // send an event, this person is an outlier
            if (Math.abs(t.f2 - average.value()) > stdev.value() * 3) {
                collector.collect(new Tuple5<>(t.f0, t.f1, t.f2, average.value()
                        , t.f3));
            }
        }

    }
}
