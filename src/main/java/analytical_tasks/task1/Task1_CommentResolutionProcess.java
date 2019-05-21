package analytical_tasks.task1;

import model.CommentEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class Task1_CommentResolutionProcess extends KeyedBroadcastProcessFunction<String, CommentEvent, CommentEvent, CommentEvent> {

    private transient ValueState<String> postID;
    private transient ValueState<Long> processedWatermark;

    // Operator State
    private HashMap<String, ArrayList<CommentEvent>> cache = new HashMap<>();
    private HashSet<String> equivalenceClass;

    @Override
    public void processElement(CommentEvent commentEvent, ReadOnlyContext readOnlyContext,
                               Collector<CommentEvent> collector) throws Exception {

        // We need this because we can't access the current key in the processBroadcast
        // function, so the state is set here.
        if (postID.value().equals("")) postID.update(readOnlyContext.getCurrentKey());

        // Directly emit it because it already has its corresponding parent postID
        emitReply(commentEvent, collector);

        // Each time a new event arrives, check whether our processed watermark is out of
        // date, if it is the case, update processed watermark and clean the buffer from all
        // buffered events with event time before the watermark, this keeps the state small
        // enough.
        if (processedWatermark.value() < readOnlyContext.timerService().currentWatermark()) {
            processedWatermark.update(readOnlyContext.timerService().currentWatermark());

            for (Map.Entry<String, ArrayList<CommentEvent>> entry : cache.entrySet()) {

                Iterator<CommentEvent> itr = entry.getValue().iterator();

                while (itr.hasNext()) {
                    CommentEvent ev = itr.next();
                    if (ev.getTimeMilisecond() < processedWatermark.value()) itr.remove();
                }

            }
        }
    }

    private void emitReply(CommentEvent commentEvent, Collector<CommentEvent> collector) throws Exception {

        // Update equivalence class state
        equivalenceClass.add(commentEvent.getId());

        collector.collect(commentEvent);

        ArrayList<CommentEvent> arr = cache.get(commentEvent.getId());

        if (arr != null) {
            for (CommentEvent event : arr) {

                CommentEvent updatedCommentEvent = new CommentEvent(event, commentEvent.getReply_to_postId());
                emitReply(updatedCommentEvent, collector);
            }
        }
    }

    @Override
    public void processBroadcastElement(CommentEvent commentEvent, Context context, Collector<CommentEvent> collector) throws Exception {

        if (equivalenceClass.contains(commentEvent.getReply_to_commentId())) {

            equivalenceClass.add(commentEvent.getId());

            CommentEvent updatedCommentEvent = new CommentEvent(commentEvent, postID.value());
            emitReply(updatedCommentEvent, collector);

        } else {

            if (cache.get(commentEvent.getReply_to_commentId()) == null) {

                ArrayList<CommentEvent> arr = new ArrayList<>();
                arr.add(commentEvent);
                cache.put(commentEvent.getReply_to_commentId(), arr);
            } else {

                ArrayList<CommentEvent> arr = cache.get(commentEvent.getReply_to_commentId());

                arr.add(commentEvent);
                cache.put(commentEvent.getReply_to_commentId(), arr);
            }
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        ValueStateDescriptor<String> postIDDescriptor =
                new ValueStateDescriptor<>("postID",
                        BasicTypeInfo.STRING_TYPE_INFO, "");

        postID = getRuntimeContext().getState(postIDDescriptor);

        ValueStateDescriptor<Long> processedWatermarkDescriptor =
                new ValueStateDescriptor<>("processedWatermark",
                        BasicTypeInfo.LONG_TYPE_INFO, 0L);

        processedWatermark = getRuntimeContext().getState(processedWatermarkDescriptor);
        equivalenceClass = new HashSet<>();
    }
}


