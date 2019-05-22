package analytical_tasks.task2;

import model.PostEvent;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.function.Consumer;

import static analytical_tasks.task2.FixedCategory.ACTIVE;
import static analytical_tasks.task2.FixedCategory.SAME_FORUM_POST;

public class PostToSameForumWindowFunction extends ProcessWindowFunction<PostEvent, ScoreHandler[],String, TimeWindow> {

    private String[] ids;
    private HashSet[] postedForums;
    private MapState<String, Integer> sameForumPosts;
    public PostToSameForumWindowFunction(String[] ids) {
        this.ids = ids;
        postedForums = new HashSet[10];
        for(int i = 0; i < 10; i++) {
            postedForums[i] = new HashSet();
        }
    }

    @Override
    public void process(String s, Context context, Iterable<PostEvent> iterable, Collector<ScoreHandler[]> collector) throws Exception {
        ScoreHandler[] scoreHandlers = new ScoreHandler[10];
        for(int i = 0; i < 10; i++) {
            scoreHandlers[i] = new ScoreHandler(ids[i]);
        }
        sameForumPosts = context.windowState().getMapState(new MapStateDescriptor<String, Integer>("posts", String.class, Integer.class));
        iterable.forEach(new Consumer<PostEvent>() {
            @Override
            public void accept(PostEvent postEvent) {
                for(int i = 0; i < 10; i++) {
                    if(postEvent.getPersonId().equals(ids[i])) {
                        postedForums[i].add(postEvent.getId());
                        break;
                    }

                    if(postedForums[i].contains(postEvent.getId())) {
                        Integer value;
                        try {
                            value = sameForumPosts.get(postEvent.getPersonId());
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
                            sameForumPosts.put(postEvent.getPersonId(), value);
                        } catch (Exception e) {
                            e.printStackTrace();
                            continue;
                        }
                        scoreHandlers[i].updateScore(postEvent.getPersonId(), SAME_FORUM_POST);
                    }
                }
                for(int i = 0; i < 10; i++) {
                    scoreHandlers[i].updateScore(postEvent.getPersonId(), ACTIVE);
                }
            }
        });
        collector.collect(scoreHandlers);
    }

    @Override
    public void clear(Context context) throws Exception {
        sameForumPosts.clear();
    }
}
