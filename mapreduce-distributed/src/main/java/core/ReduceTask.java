package core;

import com.fasterxml.jackson.annotation.JsonCreator;
import core.dataformat.DataFormat;
import core.dataformat.KvPair;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import res.Output;
import res.ResTask;

import java.io.File;
import java.util.*;

public class ReduceTask<K2,V2,K3,V3> extends ResTask {

    private int seq;
    private List<MapOutPut>  outPutList;
    private Reducer<K2,V2,K3,V3> reducer;
    private DataFormat outputFormat;

    @JsonCreator
    public ReduceTask(int seq, Reducer<K2,V2,K3,V3> reducer, List<MapOutPut> outPutList, DataFormat outputFormat) {
        this.seq = seq;
        this.reducer = reducer;
        this.outPutList = outPutList;
        this.outputFormat = outputFormat;
    }

    @Override
    public void init() {
    }

    @Override
    @SneakyThrows
    public Output run() {
        // shuffle, fetch all file and merge
        Map<K2, List<V2>> groupData = new HashMap<>();
        for (MapOutPut mapOutPut : outPutList) {
            File f = new File(mapOutPut.getTmpFiles()[seq]);
            if(f.exists()){
                FileUtils.readLines(f).forEach(line -> {
                    KvPair<K2,V2> kvPair = JacksonUtil.fromJsonString(line, KvPair.class);
                    if(groupData.containsKey(kvPair.getK())){
                        groupData.get(kvPair.getK()).add(kvPair.getV());
                    } else {
                        groupData.put(kvPair.getK(), new LinkedList<>(Collections.singletonList(kvPair.getV())));
                    }
                });
            }

        }

        // reduce
        groupData.forEach(((k2, v2s) -> {
            KvPair<K3, V3> res = reducer.reduce(k2, v2s);
            outputFormat.append(String.valueOf(seq), res);
        }));

        return new Output() {
            @Override
            public String toString() {
                return "";
            }
        };
    }

}
