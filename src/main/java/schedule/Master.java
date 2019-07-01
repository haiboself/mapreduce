package schedule;

import input.InputFormat;
import input.Partition;
import input.Record;
import lombok.Data;
import lombok.NoArgsConstructor;
import mapper.Mapper;
import output.HashPartition;
import output.Partitioner;
import reducer.Reducer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Data
@NoArgsConstructor
public class Master<K1 extends Comparable<K1>, V1, K2 extends Comparable<K2>, V2>
{
    /**
     * map 数
     */
    private int maps = 1;
    /**
     * reduce 数
     */
    private int reduces = 1;

    private Mapper<K1, V1> map;
    private Reducer<K2, V2> reduce;
    private InputFormat<K1, V1> inputFormat;
    private Partitioner<K2, V2> partitioner = new HashPartition<>();

    /**
     * cache map outputs
     */
    private HashMap<Integer,HashMap<Integer, List<Record<K2, V2>>>> inteCache = new HashMap<>(reduces);
    private HashMap<Integer,List<V2>> outputCache = new HashMap<>(reduces);

    public HashMap<Integer,List<V2>> process()
    {
        Objects.requireNonNull(map);
        Objects.requireNonNull(reduce);
        Objects.requireNonNull(inputFormat);

        // map stage
        int r = 0;
        for (Partition<K1, V1> p : inputFormat.partitions(maps)) {
            mapTask(r++,p);
        }

        // reduce stage
        for(int i = 0; i < reduces; i++){
           reduceTask(i);
        }

        // 返回结果
        return outputCache;
    }

    private void mapTask(int number, Partition<K1, V1> p) {
        Iterator<Record<K1, V1>> iterator = p.iterator();
        List<Record<K2,V2>> res = new LinkedList<>();

        while (iterator.hasNext()) {
            Record<K1, V1> record = iterator.next();
            res.addAll(map.map(record));
        }

        HashMap<Integer, List<Record<K2, V2>>> resPars = partition(res);
        inteCache.put(number,resPars);

    }

    private void reduceTask(int number) {
        List<Record<K2,V2>> data = new LinkedList<>();
        // shuffle
        for(Map.Entry<Integer,HashMap<Integer, List<Record<K2, V2>>>> entry : inteCache.entrySet()){
            data.addAll(entry.getValue().get(number));
        }

        // sort todo: group by 怎么实现比较好?
        Collections.sort(data);
        Map<K2,List<V2>> sortsData = new HashMap<>();
        for(Record<K2,V2> r : data){
            if(sortsData.containsKey(r.getK())){
                sortsData.get(r.getK()).add(r.getV());
            } else {
                List<V2> ls = new LinkedList<>();
                ls.add(r.getV());
                sortsData.put(r.getK(), ls);
            }
        }

        // reduce
        List<V2> reduceRes = new LinkedList<>();
        for(Map.Entry<K2,List<V2>> entry : sortsData.entrySet()){
            reduceRes.addAll(reduce.reduce(entry.getKey(),entry.getValue()));
        }

        // output to file
        outputCache.put(number,reduceRes);
    }

    /**
     * @param mapRes
     * @return
     */
    private HashMap<Integer, List<Record<K2, V2>>> partition(List<Record<K2, V2>> mapRes)
    {
        HashMap<Integer, List<Record<K2, V2>>> tmpFiles = new HashMap<>(reduces);
        for (Record<K2, V2> r : mapRes) {
            int k = partitioner.getPartition(r.getK(), r.getV(), reduces);
            if (tmpFiles.containsKey(k)) {
                tmpFiles.get(k).add(r);
            }
            else {
                tmpFiles.put(k, new LinkedList<>(Collections.singleton(r)));
            }
        }

        return tmpFiles;
    }
}
