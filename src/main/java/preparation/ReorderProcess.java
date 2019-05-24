package preparation;

import main.Main;
import model.Event;
import org.apache.commons.configuration2.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import utils.Tuple;

import java.util.Comparator;
import java.util.Date;
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
    // This checks if the data arrives in order and throws away out
    // of order data.
    long outOfOrderCheck;

    TreeSet<Tuple<Long, T>> buffer;

    @Override
    public void processElement(T event, Context context, Collector<T> collector) throws Exception {
        long currentProcessingTime = context.timerService().currentProcessingTime();
        if (firstElement) {
            Configuration configuration = Main.getGlobalConfig();
            verbose = configuration.getBoolean("task0Verbose");
            outOfOrderCheck = event.getTimestamp();
            buffer = new TreeSet<>(Comparator.comparingLong(t -> t.x));
            startingTimestamp = currentProcessingTime;
            if(verbose) {
                System.out.println("Starting time at: " + (new Date(startingTimestamp)).toString());
            }
            timeDifference = currentProcessingTime - event.getTimestamp();
            speedup = configuration.getLong("speedup");
            minDelayInMilli = configuration.getInt("minDelayInSec") * 1000;
            maxDelayInMilli = configuration.getInt("maxDelayInSec") * 1000;
            firstElement = false;
        } else {
            if(event.getTimestamp() < outOfOrderCheck) {
                if(verbose) {
                    System.out.println("Found out of order. Last timestamp: " +
                            (new Date(outOfOrderCheck)).toString() +
                            " Element timestamp: " +
                            (new Date(event.getTimestamp())).toString());
                }
                return;
            } else {
                outOfOrderCheck = event.getTimestamp();
            }
        }

        // Calculate time to output event.
        int randomOffset = ThreadLocalRandom.current().nextInt(minDelayInMilli, maxDelayInMilli);
        long updatedTimestamp = event.getTimestamp() + timeDifference + randomOffset;
        long timeUntilOutput = (updatedTimestamp - startingTimestamp) / speedup;
        long outputTime = startingTimestamp + timeUntilOutput;
        buffer.add(new Tuple<>(outputTime, event));
        if(verbose) {
            System.out.println("Calculated event." +
                    " Rolled Offset[secs]: " + (randomOffset/1000) +
                    " Original Timestamp: " + (new Date(event.getTimestamp())).toString() +
                    " Timestamp translated to current with offset: " + (new Date(updatedTimestamp)).toString() +
                    " Output Time: " + (new Date(outputTime).toString()));
        }

        // Check if something needs to be output.
        while (true) {
            currentProcessingTime = context.timerService().currentProcessingTime();
            if(buffer.size() == 0) {
                break;
            }
            Tuple<Long, T> iter = buffer.first();
            long iterTimestamp = iter.x;
            T iterEvent = iter.y;
            if(iterTimestamp <= currentProcessingTime) {
                collector.collect(iterEvent);
                buffer.remove(iter);
                if(verbose) {
                    long currentTime = context.timerService().currentProcessingTime();
                    System.out.println("Output." +
                            " Current Time: " + (new Date(currentProcessingTime)).toString() +
                            " Output Time: " + (new Date(iterTimestamp)).toString());
                }
            } else {
                break;
            }
        }

        // Calculate size of buffer and wait if full.
        currentProcessingTime = context.timerService().currentProcessingTime();
        if(currentProcessingTime + (maxDelayInMilli/speedup)  < event.getTimestamp() + timeDifference) {
            if(buffer.size() == 0) {
                return;
            }
            long waitingOption1 = outputTime - currentProcessingTime;
            long waitingOption2 = buffer.first().x - currentProcessingTime;
            if(waitingOption1 <= 0 || waitingOption2 <= 0) {
                return;
            }

            if(waitingOption1 > waitingOption2) {
                Thread.sleep(waitingOption2);
            } else {
                Thread.sleep(waitingOption1);
            }
        }
        return;
    }
}
