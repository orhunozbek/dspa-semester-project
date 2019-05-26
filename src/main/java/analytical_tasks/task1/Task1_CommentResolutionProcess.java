package analytical_tasks.task1;

import model.CommentEvent;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 *  A KeyedBroadcastProcessFunction which takes a KeyedStream of CommentEvents (keyed by postID) and a BroadcastStream
 *  of CommentEvents (without a postID). Finds parent postId's of CommentEvents with empty reply_to_post_Id fields. This
 *  is achieved by maintaining a mapping from commentIDs to corresponding postIDs which represents a flattened tree.
 *
 * Author: oezbeko
 */
public class Task1_CommentResolutionProcess extends KeyedBroadcastProcessFunction<String, CommentEvent, CommentEvent, CommentEvent> {

    /**
     * A mapping from commentID to a list of comment events. Each key contains a list of commentEvents without a postID
     * and with reply_to_key_id equals to key. This data structure is a staging area for comment events without a postID.
     * Each time a watermark is received, these cached comments are resolved and map to their parent postID. This is done
     * through onTimer function.
     */
    private HashMap<String, ArrayList<CommentEvent>> cache = new HashMap<>();

    /**
     * A set which contains all commentIds corresponding to a postID. Since the stream is keyed by the postID, this
     * KeyedState is also partitioned by postIDs.
     */
    private transient MapState<String,String> equivalenceClass;

    /**
     * A flag which is used to understand whether the first timer is set. Since onTimer() method registers the next timer
     * only setting the first timer is enough in processElement.
     */
    private transient ValueState<Boolean> timerSet;

    @Override
    public void processElement(CommentEvent commentEvent, ReadOnlyContext readOnlyContext,
                               Collector<CommentEvent> collector) throws Exception {

        if (!timerSet.value()) {
            readOnlyContext.timerService().registerEventTimeTimer(readOnlyContext.timerService().currentWatermark() + 1 );
            timerSet.update(true);
        }

        equivalenceClass.put(commentEvent.getId(), "");
        collector.collect(commentEvent);

    }

    /**
     *  A recursive function which adds a CommentEvent to the equivalence class of a postID, making it a part of that
     *  particular post. In addition, the cache is queried recursively so that all CommentEvents that replies this
     *  CommentEvent is also added to the equivalence class. (And also their successors)
     *
     * @param commentEvent: CommentEvent to be emitted to downstream after postID resolution.
     * @param collector: collector instance to send the event to downstream.
     */
    private void emitReply(CommentEvent commentEvent, Collector<CommentEvent> collector) throws Exception {

        equivalenceClass.put(commentEvent.getId(), "");
        collector.collect(commentEvent);

        if (null !=  cache.get(commentEvent.getId())) {
            for (CommentEvent event :  cache.get(commentEvent.getId())) {
                CommentEvent updatedCommentEvent = new CommentEvent(event, commentEvent.getReply_to_postId());
                emitReply(updatedCommentEvent, collector);
            }
        }

        cache.remove(commentEvent.getId());
    }

    @Override
    public void processBroadcastElement(CommentEvent commentEvent, Context context, Collector<CommentEvent> collector) throws Exception {

        // If cache does not have the commentID, add the entry, otherwise, add the Event to its parent comment.
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

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        MapStateDescriptor<String, String> equivalenceClassDescriptor = new MapStateDescriptor<>(
                "equivalenceClass",
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO);

        equivalenceClass = getRuntimeContext().getMapState(equivalenceClassDescriptor);

        ValueStateDescriptor<Boolean> timerSetDescriptor =
                new ValueStateDescriptor<>("timerSet",
                        BasicTypeInfo.BOOLEAN_TYPE_INFO, false);

        timerSet = getRuntimeContext().getState(timerSetDescriptor);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<CommentEvent> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        ctx.timerService().registerEventTimeTimer(ctx.timerService().currentWatermark() + 1);

        for (String entry: Lists.newArrayList(equivalenceClass.keys())){

            if (cache.containsKey(entry)){
                for (CommentEvent ev : cache.get(entry)){
                        CommentEvent updatedCommentEvent = new CommentEvent(ev, ctx.getCurrentKey());
                        emitReply(updatedCommentEvent, out);
                }
                cache.remove(entry);
            }
        }
    }


}


