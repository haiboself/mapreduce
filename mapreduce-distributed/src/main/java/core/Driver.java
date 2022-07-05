package core;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import core.dataformat.DataFormat;
import core.dataformat.Split;
import lombok.NonNull;
import lombok.Setter;
import res.ResClient;

import java.util.LinkedList;
import java.util.List;

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
       List<String> mapTasks = new LinkedList<>();
       for(Split split : inputFormat.getSplits()){
           String mapTaskId = resClient.submit(new MapTask<>(mapper, split, reducerNum)).get();
           mapTasks.add(mapTaskId);
       }

       // waiting for map finished
//       while (true) {
//           for (String mapId : mapTasks) {
//               TaskStatus status = resClient.getStatus(mapId);
//               if (status.isFail()) {
//               } else if(status.isSuccess()){
//                   Output output = resClient.getOuput(mapId);
//               }
//           }
//       }

       // reduce stage

       // waiting for  finished
    }
}
