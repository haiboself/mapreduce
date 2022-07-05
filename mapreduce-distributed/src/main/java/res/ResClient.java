package res;


import io.vavr.control.Try;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

@AllArgsConstructor
@Slf4j
public class ResClient {
    private BlockingDeque<ResTask> queue;

    public static ResClient create(){
        BlockingDeque<ResTask> queue = new LinkedBlockingDeque<>();
        ResClient resClient = new ResClient(queue);
        Starter.startLocalMaster(8888, queue);

        return resClient;
    }

    public Try<String> submit(ResTask task) {
        log.info("submit task {}", task.getTaskId());
        return Try.of(() -> queue.offer(task)).map(r -> task.getTaskId());
    }

    public TaskStatus getStatus(String mapId) {
        return null;
    }

    public Output getOuput(String mapId) {
        return null;
    }
}
