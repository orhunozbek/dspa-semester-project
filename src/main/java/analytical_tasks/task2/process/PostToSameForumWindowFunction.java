package analytical_tasks.task2.process;

import analytical_tasks.task2.ScoreHandler;
import model.PostEvent;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static analytical_tasks.task2.FixedCategory.ACTIVE;
import static analytical_tasks.task2.FixedCategory.SAME_FORUM_POST;

public class PostToSameForumWindowFunction extends ProcessWindowFunction<PostEvent, ScoreHandler[],String, TimeWindow> {

    private String[] ids;
    private MapState<Integer, String[]> sameForumPosts;
    public PostToSameForumWindowFunction(String[] ids) {
        this.ids = ids;
    }

    @Override
    public void process(String s, Context context, Iterable<PostEvent> iterable, Collector<ScoreHandler[]> collector) throws Exception {
        ScoreHandler[] scoreHandlers = new ScoreHandler[10];
        for(int i = 0; i < 10; i++) {
            scoreHandlers[i] = new ScoreHandler(ids[i]);
        }

        sameForumPosts = context.windowState().getMapState(new MapStateDescriptor<Integer, String[]>("posts", Integer.class, String[].class));
        iterable.forEach(new Consumer<PostEvent>() {
            @Override
            public void accept(PostEvent postEvent) {
                for(int i = 0; i < 10; i++) {
                    String[] forumPosts;
                    try {
                        forumPosts = sameForumPosts.get(i);
                    } catch (Exception e) {
                        e.printStackTrace();
                        return;
                    }
                    if(forumPosts == null) {
                        forumPosts = new String[0];
                    }
                    if(postEvent.getPersonId().equals(ids[i])) {
                        List<String> temp = Arrays.asList(forumPosts);
                        temp.add(postEvent.getId());
                        try {
                            sameForumPosts.put(i, (String[]) temp.toArray());
                        } catch (Exception e) {
                            e.printStackTrace();
                            continue;
                        }
                        break;
                    }

                    if(Arrays.asList(forumPosts).contains(postEvent.getId())) {
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
