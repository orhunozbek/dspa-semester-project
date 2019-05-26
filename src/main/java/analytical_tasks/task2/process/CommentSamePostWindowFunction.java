package analytical_tasks.task2.process;

import analytical_tasks.task2.ScoreHandler;
import model.CommentEvent;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static analytical_tasks.task2.FixedCategory.*;

public class CommentSamePostWindowFunction extends ProcessWindowFunction<CommentEvent, ScoreHandler[], String, TimeWindow> {

    private String[] ids;
    private MapState<Integer, String[]> commentedSamePost;
    private MapState<Integer, String[]> commentedSameComment;

    public CommentSamePostWindowFunction(String[] ids) {
        this.ids = ids;
    }

    @Override
    public void process(String s, Context context, Iterable<CommentEvent> iterable, Collector<ScoreHandler[]> collector) throws Exception {
        ScoreHandler[] scoreHandlers = new ScoreHandler[10];
        for (int i = 0; i < 10; i++) {
            scoreHandlers[i] = new ScoreHandler(ids[i]);
        }
        commentedSamePost = context.windowState().getMapState(new MapStateDescriptor<>("commentedSamePost", Integer.class, String[].class));
        commentedSameComment = context.windowState().getMapState(new MapStateDescriptor<>("commentedSameComment", Integer.class, String[].class));
        iterable.forEach(commentEvent -> {
            for (int i = 0; i < 10; i++) {
                String[] commentedPosts;
                String[] commentedComment;
                try {
                    commentedPosts = commentedSamePost.get(i);
                    commentedComment = commentedSameComment.get(i);
                } catch (Exception e) {
                    e.printStackTrace();
                    continue;
                }
                if (commentedPosts == null) {
                    commentedPosts = new String[0];
                    commentedComment = new String[0];
                }
                if (commentEvent.getPersonId().equals(ids[i])) {
                    if (commentEvent.getReply_to_postId() == null || commentEvent.getReply_to_postId().equals("")) {
                        List<String> temp = new ArrayList(Arrays.asList(commentedComment));
                        temp.add(commentEvent.getReply_to_commentId());
                        try {
                            commentedSameComment.put(i, convertToArray(temp));
                        } catch (Exception e) {
                            e.printStackTrace();
                            continue;
                        }
                        continue;
                    } else {
                        List<String> temp = new ArrayList(Arrays.asList(commentedPosts));
                        temp.add(commentEvent.getReply_to_postId());
                        try {
                            commentedSamePost.put(i, convertToArray(temp));
                        } catch (Exception e) {
                            e.printStackTrace();
                            continue;
                        }
                        continue;
                    }
                }

                if (Arrays.asList(commentedComment).contains(commentEvent.getReply_to_commentId())) {
                    scoreHandlers[i].updateScore(commentEvent.getPersonId(), COMMENTED_SAME_COMMENT);
                }

                if (Arrays.asList(commentedPosts).contains(commentEvent.getReply_to_postId())) {
                    scoreHandlers[i].updateScore(commentEvent.getPersonId(), COMMENTED_SAME_POST);
                }
            }
        });
        collector.collect(scoreHandlers);
    }

    @Override
    public void clear(Context context){
        commentedSameComment.clear();
        commentedSamePost.clear();
    }

    public String[] convertToArray(List<String> list) {
        String[] result = new String[list.size()];
        for (int i = 0; i < list.size(); i++) {
            result[i] = list.get(i);
        }
        return result;
    }
}
