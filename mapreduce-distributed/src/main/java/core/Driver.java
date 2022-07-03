package core;

import com.typesafe.config.Config;
import core.dataformat.DataFormat;
import core.dataformat.Split;
import res.Output;
import res.ResClient;
import res.TaskStatus;

import java.util.LinkedList;
import java.util.List;

public class Driver {

    private int reducerNum =  1;

    private List<Mapper> mapper = new LinkedList<>();
    private Reducer reducer;
    private Reducer combiner;

    private DataFormat inputFormat;
    private DataFormat outputFormat;

    private Config conf;

    private ResClient resClient;
    public void submit(){

       // map stage
       List<String> mapTasks = new LinkedList<>();
       for(Split split : inputFormat.getSplits()){
           String mapTaskId = resClient.submit(new MapTask(split));
           mapTasks.add(mapTaskId);
       }

       // waiting for map finished
       while (true) {
           for (String mapId : mapTasks) {
               TaskStatus status = resClient.getStatus(mapId);
               if (status.isFail()) {
               } else if(status.isSuccess()){
                   Output output = resClient.getOuput(mapId);
               }
           }
       }

       // reduce stage

       // waiting for map finished
    }
}
