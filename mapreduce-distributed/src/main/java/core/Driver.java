package core;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import core.dataformat.DataFormat;
import core.dataformat.Split;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import rsm.ResClient;
import rsm.TaskStatus;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

@Slf4j
@Setter
public class Driver<K1,V1,K2,V2,K3,V3>  {

    private int reducerNum =  1;

    @NonNull
    private Mapper<K1,V1,K2,V2> mapper;
    @NonNull
    private Reducer<K2,V2,K3,V3> reducer;
    private Reducer<K2,V2,K3,V3> combiner;

    @NonNull
    private DataFormat inputFormat;
    @NonNull
    private DataFormat outputFormat;
    private Config conf;

    private ResClient resClient;

    public Driver(){
        init();
    }

    private void init(){
        this.resClient = ResClient.create();
        this.conf = ConfigFactory.load();
    }

    public void submit(){

       // map stage
       log.info("start map stage====================");
       List<String> mapTasks = new LinkedList<>();
       for(Split split : inputFormat.getSplits()){
           String mapTaskId = resClient.submit(new MapTask<>(mapper, split, reducerNum)).get();
           mapTasks.add(mapTaskId);
           log.info("run map task {}", mapTaskId);
       }

       // waiting for map finished
       List<MapOutPut> mapOutPuts = new LinkedList<>();
       while (!mapTasks.isEmpty()) {
           Iterator<String> iterator = mapTasks.iterator();
           while (iterator.hasNext()){
               String mapId = iterator.next();
               TaskStatus status = resClient.getStatus(mapId);
               if (status.isFail()) {
                   log.info("map {} fail", mapId);
                   iterator.remove();
               } else if(status.isSuccess()){
                   log.info("map {} success", mapId);
                   iterator.remove();
                   MapOutPut output = resClient.getOutput(mapId, MapOutPut.class).get();
                   mapOutPuts.add(output);
               }
           }
       }
       log.info("end map stage====================");

       // reduce stage
        log.info("start reduce stage====================");
        List<String> reduceTasks = new LinkedList<>();
       for(int i = 0; i < reducerNum; i++){
           String reduceTaskId = resClient.submit(new ReduceTask<>(i, reducer, mapOutPuts, outputFormat)).get();
           reduceTasks.add(reduceTaskId);
       }

       // waiting for reduce finished
        while (!reduceTasks.isEmpty()) {
            Iterator<String> iterator = reduceTasks.iterator();
            while (iterator.hasNext()){
                String reduceId = iterator.next();
                TaskStatus status = resClient.getStatus(reduceId);
                if (status.isFail()) {
                    log.info("reduce {} fail", reduceId);
                    iterator.remove();
                } else if(status.isSuccess()){
                    log.info("reduce {} success", reduceId);
                    iterator.remove();
                }
            }
        }

        log.info("end reduce stage====================");
    }
}
