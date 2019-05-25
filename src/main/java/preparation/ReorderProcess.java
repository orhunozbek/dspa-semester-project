package preparation;

import main.Main;
import model.Event;
import org.apache.commons.configuration2.Configuration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;

public class ReorderProcess<T extends Event> extends ProcessFunction<T, T> {

    // Set this in config.properties to output verification output.
    boolean verbose;
    // True until the first element is processed.
    boolean firstElement = true;
    // Time delta between the data and the processing time.
    long timeDifference;
    // The speedup
    long speedup;
    // Minimum and maximum delay according to config.properties.
    int minDelayInMilli;
    int maxDelayInMilli;
    // The time output starts.
    long startingTimestamp;

    TreeSet<Tuple2<Long, T>> buffer;

    @Override
    public void processElement(T event, Context context, Collector<T> collector) throws Exception {
        long currentProcessingTime = context.timerService().currentProcessingTime();
        if (firstElement) {
            Configuration configuration = Main.getGlobalConfig();
            verbose = configuration.getBoolean("task0Verbose");
            buffer = new TreeSet<>(Comparator.comparingLong(t -> t.f0));
            startingTimestamp = currentProcessingTime;
            timeDifference = currentProcessingTime - event.getTimestamp();
            speedup = configuration.getLong("speedup");
            minDelayInMilli = configuration.getInt("minDelayInSec") * 1000;
            maxDelayInMilli = configuration.getInt("maxDelayInSec") * 1000;
            firstElement = false;
        }
        // Calculate time to output event.

        int randomOffset;
        if (minDelayInMilli != maxDelayInMilli) {
            randomOffset = ThreadLocalRandom.current().nextInt(minDelayInMilli, maxDelayInMilli);
        } else {
            randomOffset = minDelayInMilli;
        }
        long updatedTimestamp = event.getTimestamp() + timeDifference + randomOffset;
        long timeUntilOutput = (updatedTimestamp - startingTimestamp) / speedup;
        long outputTime = startingTimestamp + timeUntilOutput;
        while (buffer.contains(new Tuple2<>(outputTime, null))) {
            outputTime = outputTime + 1;
        }
        buffer.add(new Tuple2<>(outputTime, event));
        output(context, collector);

        // Calculate size of buffer and wait if full.
        currentProcessingTime = context.timerService().currentProcessingTime();
        if(currentProcessingTime + (maxDelayInMilli/speedup)  < event.getTimestamp() + timeDifference) {
            if(buffer.size() == 0) {
                return;
            }
            long waitingOption1 = outputTime - currentProcessingTime;
            long waitingOption2 = buffer.first().f0 - currentProcessingTime;
            if(waitingOption1 <= 0 || waitingOption2 <= 0) {
                return;
            }

            if(waitingOption1 > waitingOption2) {
                Thread.sleep(waitingOption2);
            } else {
                Thread.sleep(waitingOption1);
            }
        }
    }

    private void output(Context context, Collector<T> collector) {
        long currentProcessingTime;// Check if something needs to be output.
        while (true) {
            currentProcessingTime = context.timerService().currentProcessingTime();
            if(buffer.size() == 0) {
                break;
            }
            Tuple2<Long, T> iter = buffer.first();
            long iterTimestamp = iter.f0;
            T iterEvent = iter.f1;
            if(iterTimestamp <= currentProcessingTime) {
                collector.collect(iterEvent);
                buffer.remove(iter);
            } else {
                break;
            }
        }
    }
}
