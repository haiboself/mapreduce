package res;

import akka.actor.typed.Behavior;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.javadsl.*;
import akka.actor.typed.ActorRef;
import akka.actor.typed.receptionist.Receptionist;
import org.slf4j.Logger;

import java.util.HashSet;
import java.util.Set;

public class MasterActor extends AbstractBehavior<MasterActor.MasterEvent> {
    interface MasterEvent extends CborSerializable {}

    private static final class WorkerUpdated implements MasterEvent {
        public final Set<ActorRef<WorkerActor.WorkerEvent>> newWorkers;

        private WorkerUpdated(Set<ActorRef<WorkerActor.WorkerEvent>> newWorkers) {
            this.newWorkers = newWorkers;
        }
    }

    private final Set<ActorRef<WorkerActor.WorkerEvent>> workers = new HashSet<>();
    private final Logger logger = getContext().getLog();

    private MasterActor(ActorContext<MasterEvent> context, TimerScheduler<MasterEvent> timers) {
        super(context);

        ActorRef<Receptionist.Listing> subsAdapter = context.messageAdapter(Receptionist.Listing.class, listing ->
                new WorkerUpdated(listing.getServiceInstances(WorkerActor.SERVICE_KEY)));

        context.getSystem()
                .receptionist()
                .tell(Receptionist.subscribe(WorkerActor.SERVICE_KEY, subsAdapter));
    }

    public static Behavior<MasterEvent> create() {
        return Behaviors.<MasterEvent>supervise(Behaviors.setup(context ->
            Behaviors.withTimers(timers -> new MasterActor(context, timers))))
                .onFailure(SupervisorStrategy.restart());
    }

    @Override
    public Receive<MasterEvent> createReceive() {
        return newReceiveBuilder()
                .onMessage(WorkerUpdated.class, this::onWorkersUpdated)
                .build();
    }

    private Behavior<MasterEvent> onWorkersUpdated(WorkerUpdated event){
        logger.info("workers: {}", event.newWorkers);
        workers.clear();
        workers.addAll(event.newWorkers);
        return this;
    }
}
