package res;


import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Starter {
    public static void main(String[] args) {
        startLocalMaster(8888);
        // startLocalWorker(8887);
        // startLocalWorker(8886);
    }


    static void startLocalMaster(int port){
        Map<String,Object> overrides = new HashMap<>();
        overrides.put("akka.remote.artery.canonical.port", port);
        overrides.put("akka.cluster.roles", Collections.singleton("master"));
        overrides.put("akka.actor.provider", "cluster");

        Config config = ConfigFactory.parseMap(overrides).withFallback(ConfigFactory.load());

        ActorSystem<Void> system = ActorSystem.create(Behaviors.setup(context -> {
            context.spawn(MasterActor.create(), "master");
            return Behaviors.empty();
        }), "ClusterSystem", config);
    }

    static void startLocalWorker(int port){
        Map<String,Object> overrides = new HashMap<>();
        overrides.put("akka.remote.artery.canonical.port", port);
        overrides.put("akka.cluster.roles", Collections.singleton("worker"));
        overrides.put("akka.actor.provider", "cluster");

        Config config = ConfigFactory.parseMap(overrides).withFallback(ConfigFactory.load());

        ActorSystem system = ActorSystem.create(Behaviors.setup(context -> {
            context.spawn(WorkerActor.create(), "worker");
            return Behaviors.empty();
        }), "ClusterSystem", config);
    }
}
