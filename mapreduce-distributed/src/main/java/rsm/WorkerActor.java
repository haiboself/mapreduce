package rsm;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.javadsl.*;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import akka.cluster.ClusterEvent;
import akka.cluster.typed.Cluster;
import akka.cluster.typed.Subscribe;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import core.JacksonUtil;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;

import java.io.File;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class WorkerActor extends AbstractBehavior<WorkerActor.WorkerEvent> {

    interface WorkerEvent extends CborSerializable{}
    private static final class ClusterNodeStatus implements WorkerEvent {
        final ClusterEvent.MemberEvent memberEvent;

        private ClusterNodeStatus(ClusterEvent.MemberEvent memberEvent) {
            this.memberEvent = memberEvent;
        }
    }

    public static final class StartTask implements WorkerEvent {
        public ActorRef<MasterActor.MasterEvent> master;
        public ResTask resTask;

        @JsonCreator
        public StartTask(ActorRef<MasterActor.MasterEvent> master, ResTask resTask){
            this.master = master;
            this.resTask = resTask;
        }
    }

    private final Logger logger = getContext().getLog();
    private Config config = ConfigFactory.load();
    public static final ServiceKey<WorkerEvent> SERVICE_KEY = ServiceKey.create(WorkerEvent.class, "worker");
    private final ExecutorService slots = Executors.newFixedThreadPool(10);

    private WorkerActor(ActorContext<WorkerEvent> context, TimerScheduler<WorkerEvent> timers) {
        super(context);

        // init worker
        Cluster cluster = Cluster.get(context.getSystem());
        ActorRef<ClusterEvent.MemberEvent> memberEventAdapter =
                context.messageAdapter(ClusterEvent.MemberEvent.class, ClusterNodeStatus::new);
        cluster.subscriptions().tell(Subscribe.create(memberEventAdapter, ClusterEvent.MemberEvent.class));

        // subscribe to master
        context.getSystem().receptionist().tell(Receptionist.register(SERVICE_KEY, context.getSelf().narrow()));
    }

    public static Behavior<WorkerEvent> create() {
        return Behaviors.<WorkerEvent>supervise(Behaviors.setup(context ->
                Behaviors.withTimers(timers -> new WorkerActor(context, timers))))
                .onFailure(SupervisorStrategy.restart());
    }

    @Override
    public Receive<WorkerEvent> createReceive() {
        return newReceiveBuilder()
                .onMessage(StartTask.class, s -> this.startTask(s.resTask))
                .build();
    }

    private Behavior<WorkerEvent> startTask(ResTask task) {
        logger.info("run task {}", task.desc());
        updateTaskStatus(task, TaskStatus.Run);

        CompletableFuture.supplyAsync(task::run, slots)
                .thenAccept(r -> {
                    logger.info("success task {}", task.desc());
                    saveTaskOutput(task, r);
                    updateTaskStatus(task, TaskStatus.Success);
                })
                .exceptionally(e -> {
                    logger.info("fail task {}", task.desc(), e);
                    updateTaskStatus(task, TaskStatus.Fail);
                    return null;
                });

        return this;
    }

    @SneakyThrows
    private void saveTaskOutput(ResTask task, Output output) {
        String path = config.getString("res.task.info.baseDir")  + "/" + task.getTaskId() + "/output.txt";
        FileUtils.writeStringToFile(new File(path), JacksonUtil.toJsonString(output));
        logger.info("save task {} output in {}", task.desc(), path);
    }

    @SneakyThrows
    private void updateTaskStatus(ResTask task, TaskStatus status) {
        String path = config.getString("res.task.info.baseDir")  + "/" + task.getTaskId() + "/status.txt";
        FileUtils.writeStringToFile(new File(path), JacksonUtil.toJsonString(status));
        logger.info("save task {} status in {}", task.desc(), path);
    }
}
