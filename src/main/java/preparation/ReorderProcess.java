package preparation;

import main.Main;
import model.Event;
import org.apache.commons.configuration2.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;

public class ReorderProcess<T extends Event> extends ProcessFunction<T, T> {

    boolean firstElement = true;
    long timeDifference;
    long speedup;
    int minDelayInMilli;
    int maxDelayInMilli;
    long startingTimestamp;
    long outOfOrderCheck;

    TreeSet<Tuple<Long, T>> buffer;

    @Override
    public void processElement(T event, Context context, Collector<T> collector) throws Exception {
        long currentProcessingTime = context.timerService().currentProcessingTime();
        if (firstElement) {
            outOfOrderCheck = event.getTimestamp();
            buffer = new TreeSet<>(Comparator.comparingLong(t -> t.x));
            startingTimestamp = currentProcessingTime;
            timeDifference = currentProcessingTime - event.getTimestamp();
            Configuration configuration = Main.getGlobalConfig();
            speedup = configuration.getLong("speedup");
            minDelayInMilli = configuration.getInt("minDelayInSec") * 1000;
            maxDelayInMilli = configuration.getInt("maxDelayInSec") * 1000;
            firstElement = false;
        } else {
            if(event.getTimestamp() < outOfOrderCheck) {
                System.out.println("OUT OF ORDER!");
                return;
            } else {
                outOfOrderCheck = event.getTimestamp();
            }
        }

        int randomOffset = ThreadLocalRandom.current().nextInt(minDelayInMilli, maxDelayInMilli);
        long updatedTimestamp = event.getTimestamp() + timeDifference + randomOffset;
        long timeUntilOutput = (updatedTimestamp - startingTimestamp) / speedup;
        updatedTimestamp = startingTimestamp + timeUntilOutput;
        buffer.add(new Tuple<>(updatedTimestamp, event));
        // Check if something needs to be output.
        while (true) {
            currentProcessingTime = context.timerService().currentProcessingTime();
            if(buffer.size() == 0) {
                break;
            }
            Tuple<Long, T> iter = buffer.first();
            long iterTimestamp = iter.x;
            T iterEvent = iter.y;
            if(iterTimestamp < currentProcessingTime) {
                collector.collect(iterEvent);
                buffer.remove(iter);
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
            long waitingOption1 = updatedTimestamp - currentProcessingTime;
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

    public class Tuple<X, Y> {
        public final X x;
        public final Y y;
        public Tuple(X x, Y y) {
            this.x = x;
            this.y = y;
        }
    }
}
