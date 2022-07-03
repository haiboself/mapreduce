package res;


import akka.actor.typed.ActorRef;

public class ResClient {
    private ActorRef<MasterActor.MasterEvent> masterActor;

    public ResClient(){
    }

    public String submit(ResTask mapTask) {
        return null;
    }

    public TaskStatus getStatus(String mapId) {
        return null;
    }

    public Output getOuput(String mapId) {
        return null;
    }
}
