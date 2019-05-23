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

public class Task1_CommentResolutionProcess extends KeyedBroadcastProcessFunction<String, CommentEvent, CommentEvent, CommentEvent> {

    private HashMap<String, ArrayList<CommentEvent>> cache = new HashMap<>();

    private transient MapState<String,String> equivalenceClass;
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

    private void emitReply(CommentEvent commentEvent, Collector<CommentEvent> collector) throws Exception {

        if (!equivalenceClass.contains(commentEvent.getId())) {
            // Update equivalence class state
            equivalenceClass.put(commentEvent.getId(), "");
            collector.collect(commentEvent);

            ArrayList<CommentEvent> arr = cache.get(commentEvent.getId());

            if (arr != null) {
                for (CommentEvent event : arr) {

                    CommentEvent updatedCommentEvent = new CommentEvent(event, commentEvent.getReply_to_postId());
                    emitReply(updatedCommentEvent, collector);
                }
            }
        }
    }

    @Override
    public void processBroadcastElement(CommentEvent commentEvent, Context context, Collector<CommentEvent> collector) throws Exception {

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

                ArrayList<CommentEvent> evs = new ArrayList<>(cache.get(entry));
                ArrayList<CommentEvent> toRemove = new ArrayList<>();

                for (CommentEvent ev : evs){
                        toRemove.add(ev);
                        CommentEvent updatedCommentEvent = new CommentEvent(ev, ctx.getCurrentKey());
                        emitReply(updatedCommentEvent, out);
                }

                cache.get(entry).removeAll(toRemove);
            }
        }
    }


}


