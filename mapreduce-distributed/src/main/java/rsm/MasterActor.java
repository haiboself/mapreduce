package rsm;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.javadsl.*;
import akka.actor.typed.receptionist.Receptionist;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingDeque;

public class MasterActor extends AbstractBehavior<MasterActor.MasterEvent> {

    interface MasterEvent extends CborSerializable {}
    private enum Tick implements MasterEvent {INSTANCE}

    private static final class WorkerUpdated implements MasterEvent {
        public final Set<ActorRef<WorkerActor.WorkerEvent>> newWorkers;

        private WorkerUpdated(Set<ActorRef<WorkerActor.WorkerEvent>> newWorkers) {
            this.newWorkers = newWorkers;
        }
    }

    private final Set<ActorRef<WorkerActor.WorkerEvent>> workers = new HashSet<>();
    private final Logger logger = getContext().getLog();
    private final BlockingDeque<ResTask> queue;

    private MasterActor(ActorContext<MasterEvent> context, TimerScheduler<MasterEvent> timers, BlockingDeque<ResTask> queue) {
        super(context);
        this.queue = queue;

        ActorRef<Receptionist.Listing> subsAdapter = context.messageAdapter(Receptionist.Listing.class, listing ->
                new WorkerUpdated(listing.getServiceInstances(WorkerActor.SERVICE_KEY)));

        context.getSystem()
                .receptionist()
                .tell(Receptionist.subscribe(WorkerActor.SERVICE_KEY, subsAdapter));

        timers.startTimerWithFixedDelay(Tick.INSTANCE, Tick.INSTANCE, Duration.ofMillis(200));
    }

    public static Behavior<MasterEvent> create(BlockingDeque<ResTask> queue) {
        return Behaviors.<MasterEvent>supervise(Behaviors.setup(context ->
            Behaviors.withTimers(timers -> new MasterActor(context, timers, queue))))
                .onFailure(SupervisorStrategy.restart());
    }

    @Override
    public Receive<MasterEvent> createReceive() {
        return newReceiveBuilder()
                .onMessage(WorkerUpdated.class, this::onWorkersUpdated)
                .onMessage(Tick.class, this::schedule)
                .build();
    }

    private Behavior<MasterEvent> schedule(Tick t) {

        while (!queue.isEmpty() && !workers.isEmpty()){
            // select worker
            ActorRef<WorkerActor.WorkerEvent> worker = new ArrayList<>(workers).get(
                    new Random().nextInt() % workers.size()
            );

            // get task
            ResTask task = queue.poll();

            // distribute task
            logger.info("submit task {} to worker {}", task.desc(), worker.path().toString());
            worker.tell(new WorkerActor.StartTask(getContext().getSelf(), task));
        }

        return this;
    }

    private Behavior<MasterEvent> onWorkersUpdated(WorkerUpdated event){
        logger.info("workers: {}", event.newWorkers);
        workers.clear();
        workers.addAll(event.newWorkers);
        return this;
    }
}
