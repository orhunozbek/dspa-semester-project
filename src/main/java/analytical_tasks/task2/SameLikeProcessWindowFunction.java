package analytical_tasks.task2;

import model.LikeEvent;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.function.Consumer;

import static analytical_tasks.task2.FixedCategory.ACTIVE;
import static analytical_tasks.task2.FixedCategory.LIKED_THE_SAME;

public class SameLikeProcessWindowFunction extends ProcessWindowFunction<LikeEvent, ScoreHandler[],String,TimeWindow> {

    private String[] ids;
    private HashSet[] likedPosts;
    private MapState<String, Integer> sameLikes;
    public SameLikeProcessWindowFunction(String[] ids) {
        this.ids = ids;
        likedPosts = new HashSet[10];
        for(int i = 0; i < 10; i++) {
            likedPosts[i] = new HashSet();
        }
    }

    @Override
    public void process(String postId, Context context, Iterable<LikeEvent> iterable, Collector<ScoreHandler[]> collector) throws Exception {
        ScoreHandler[] scoreHandlers = new ScoreHandler[10];
        for(int i = 0; i < 10; i++) {
            scoreHandlers[i] = new ScoreHandler(ids[i]);
        }
        sameLikes = context.windowState().getMapState(new MapStateDescriptor("likes", String.class, Integer.class));
        iterable.forEach(new Consumer<LikeEvent>() {
            @Override
            public void accept(LikeEvent likeEvent) {
                boolean hit = false;
                for(int i = 0; i < 10; i++) {
                    if(likeEvent.getPostId().equals(ids[i])) {
                        likedPosts[i].add(likeEvent.getPostId());
                        hit = true;
                        break;
                    }

                    if(likedPosts[i].contains(likeEvent.getPostId())) {
                        hit = true;
                        Integer value;
                        try {
                             value = sameLikes.get(likeEvent.getPersonId());
                        } catch (Exception e) {
                            e.printStackTrace();
                            continue;
                        }
                        if(value == null) {
                            value = 1;
                        } else {
                            value = value + 1;
                        }
                        try {
                            sameLikes.put(likeEvent.getPersonId(), value);
                        } catch (Exception e) {
                            e.printStackTrace();
                            continue;
                        }
                        scoreHandlers[i].updateScore(likeEvent.getPersonId(), LIKED_THE_SAME);
                    }
                }
//                    Integer value;
//                    try {
//                         value = sameLikes.get(likeEvent.getPersonId());
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                        return;
//                    }
//                    if(value != null) {
//                        collector.collect(new Tuple2(likeEvent.getPersonId(), value));
//                    } else {
//                        collector.collect(new Tuple2(likeEvent.getPersonId(), 0));
//                    }
                for(int i = 0; i < 10; i++ ){
                    scoreHandlers[i].updateScore(likeEvent.getPersonId(), ACTIVE);
                }
            }


        });
    }

    @Override
    public void clear(Context context) throws Exception {
        sameLikes.clear();
    }
}