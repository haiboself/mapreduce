package core;

import com.typesafe.config.Config;
import core.dataformat.DataFormat;
import res.ResClient;

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
           String mapId = resClient.submit(new MapTask(split)).get();
           mapTasks.add(taskId);
       }

       // waiting for map finished
       while (true) {
           for (String mapId : mapTasks) {
               TaskStatus status = resClient.getStatus(mapId);
               if (status.isFail) {
                   retry;
               } else if(status.isSuccess){
                   Output output = resClient.getOuput(mapId);
               }
           }
       }

       // reduce stage
        List<String> reduceTasks = new LinkedList<>();
       for(int i = 0; i < reducerNum; i++){
           String reduceId = resClient.submit(new ReduceTask(i, output));
       }

       // waiting for map finished
    }
}
