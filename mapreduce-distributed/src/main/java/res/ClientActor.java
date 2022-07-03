package res;

import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Receive;

public class ClientActor extends AbstractBehavior<ClientActor.ClientEvent> {
    public ClientActor(ActorContext<ClientEvent> context) {
        super(context);
    }

    public interface ClientEvent extends CborSerializable {}

    private MasterActor masterActor;

    @Override
    public Receive createReceive() {
        return null;
    }
}
