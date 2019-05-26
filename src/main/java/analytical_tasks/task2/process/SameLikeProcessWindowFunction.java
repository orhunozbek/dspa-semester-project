package analytical_tasks.task2.process;

import analytical_tasks.task2.ScoreHandler;
import model.LikeEvent;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static analytical_tasks.task2.FixedCategory.ACTIVE;
import static analytical_tasks.task2.FixedCategory.LIKED_THE_SAME;

public class SameLikeProcessWindowFunction extends ProcessWindowFunction<LikeEvent, ScoreHandler[], String, TimeWindow> {

    private String[] ids;
    private MapState<Integer, String[]> likedPosts;

    public SameLikeProcessWindowFunction(String[] ids) {
        this.ids = ids;
    }

    @Override
    public void process(String postId, Context context, Iterable<LikeEvent> iterable, Collector<ScoreHandler[]> collector) throws Exception {
        ScoreHandler[] scoreHandlers = new ScoreHandler[10];
        for (int i = 0; i < 10; i++) {
            scoreHandlers[i] = new ScoreHandler(ids[i]);
        }

        likedPosts = context.windowState().getMapState(new MapStateDescriptor("likedSamePost", Integer.class, String[].class));
        iterable.forEach(new Consumer<LikeEvent>() {
            @Override
            public void accept(LikeEvent likeEvent) {
                for (int i = 0; i < 10; i++) {
                    String[] likes;
                    try {
                        likes = likedPosts.get(i);
                    } catch (Exception e) {
                        e.printStackTrace();
                        continue;
                    }
                    if (likes == null) {
                        likes = new String[0];
                    }

                    if (likeEvent.getPersonId().equals(ids[i])) {
                        List<String> temp = new ArrayList(Arrays.asList(likes));
                        temp.add(likeEvent.getPostId());
                        try {
                            likedPosts.put(i, convertToArray(temp));
                        } catch (Exception e) {
                            e.printStackTrace();
                            continue;
                        }
                        break;
                    }

                    if (Arrays.asList(likedPosts).contains(likeEvent.getPostId())) {
                        scoreHandlers[i].updateScore(likeEvent.getPersonId(), LIKED_THE_SAME);
                    }
                }
            }
        });
        collector.collect(scoreHandlers);
    }

    @Override
    public void clear(Context context) throws Exception {
        likedPosts.clear();
    }

    public String[] convertToArray(List<String> list) {
        String[] result = new String[list.size()];
        for (int i = 0; i < list.size(); i++) {
            result[i] = list.get(i);
        }
        return result;
    }
}