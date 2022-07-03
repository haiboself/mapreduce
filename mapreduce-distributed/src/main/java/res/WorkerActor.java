package res;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.javadsl.*;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import akka.cluster.ClusterEvent;
import akka.cluster.typed.Cluster;
import akka.cluster.typed.Subscribe;

public class WorkerActor extends AbstractBehavior<WorkerActor.WorkerEvent> {

    interface WorkerEvent extends CborSerializable{}
    private static final class ClusterNodeStatus implements WorkerEvent {
        final ClusterEvent.MemberEvent memberEvent;

        private ClusterNodeStatus(ClusterEvent.MemberEvent memberEvent) {
            this.memberEvent = memberEvent;
        }
    }

    public static final ServiceKey<WorkerEvent> SERVICE_KEY = ServiceKey.create(WorkerEvent.class, "worker");

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
        return newReceiveBuilder().build();
    }
}
