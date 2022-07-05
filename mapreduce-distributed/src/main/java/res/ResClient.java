package res;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import core.JacksonUtil;
import io.vavr.control.Try;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.Optional;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

@AllArgsConstructor
@Slf4j
public class ResClient {
    private BlockingDeque<ResTask> queue;
    private static Config config = ConfigFactory.load();

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

    @SneakyThrows
    public TaskStatus getStatus(String mapId) {
        String path = config.getString("res.task.info.baseDir")  + "/" + mapId + "/status.txt";
        if(new File(path).exists()) {
            return Try.of(() ->
                    JacksonUtil.fromJsonString(FileUtils.readFileToString(new File(path)), TaskStatus.class)
            ).getOrElse(TaskStatus.Run);
        } else {
            return TaskStatus.Run;
        }
    }

    @SneakyThrows
    public <T extends Output> Optional<T> getOutput(String mapId, Class<T> cls) {
        String path = config.getString("res.task.info.baseDir")  + "/" + mapId + "/output.txt";
        if(new File(path).exists()) {
            return Optional.of(JacksonUtil.fromJsonString(FileUtils.readFileToString(new File(path)), cls));
        } else {
            return Optional.empty();
        }
    }
}
