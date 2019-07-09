package schedule;

import dataformat.DataFormat;
import dataformat.Partition;
import dataformat.Record;
import dataformat.RecordReader;
import lombok.Data;
import lombok.NoArgsConstructor;
import mapper.Mapper;
import dataformat.HashPartition;
import dataformat.Partitioner;
import reducer.Reducer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EventListener;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Data
@NoArgsConstructor
public class Driver<K1, V1, K2, V2, K3, V3>
{

    private Conf conf;
    /**
     * reduce 数
     */
    private int reduces = 1;

    private Mapper<K1, V1, K2, V2> mapper;
    private Reducer<K2, V2, K3, V3> reducer;
    private Reducer<K2, V2, K2, V2> combiner;

    private DataFormat<K1, V1, K3, V3> inputFormat;
    private DataFormat<K1, V1, K3, V3> outputFormat;
    private Partitioner partitioner = new HashPartition();

    /**
     * cache map outputs
     */
    private HashMap<Integer, HashMap<Integer, List<Record<K2,V2>>>> inteCache = new HashMap<>(reduces);
    private HashMap<Integer, DataFormat<K1, V1, K3, V3>> outputCache = new HashMap<>(reduces);

    public HashMap<Integer, DataFormat<K1, V1, K3, V3>> submit()
    {
        Objects.requireNonNull(mapper);
        Objects.requireNonNull(reducer);
        Objects.requireNonNull(inputFormat);
        Objects.requireNonNull(outputFormat);

        // map stage
        int r = 0;
        for (Partition p : inputFormat.partitions(conf)) {
            mapTask(r++, inputFormat.getRecordReader(p));
        }

        // reduce stage
        for (int i = 0; i < reduces; i++) {
            reduceTask(i);
        }

        // 返回结果
        return outputCache;
    }

    private void mapTask(int number,  RecordReader<K1,V1> reader)
    {
        List<Record<K2, V2>> res = new LinkedList<>();

        while (reader.hasNext()) {
            res.addAll(mapper.map(reader.getCurKey(),reader.getCurVal()));
        }

        if(combiner != null){
            // combiner
            HashMap<K2,List<V2>> groups = groupByKey(res);
            List<Record<K2,V2>> comRes = new LinkedList<>();
            for (Map.Entry<K2,List<V2>> entry : groups.entrySet()){
                comRes.add(combiner.reduce(entry.getKey(),entry.getValue()));
            }

            // partition
            HashMap<Integer, List<Record<K2, V2>>> resPars = partition(comRes);
            inteCache.put(number, resPars);
        } else {
            // partition
            HashMap<Integer, List<Record<K2, V2>>> resPars = partition(res);
            inteCache.put(number, resPars);
        }
    }

    private void reduceTask(int number)
    {
        List<Record<K2, V2>> data = new LinkedList<>();
        // shuffle
        for (Map.Entry<Integer, HashMap<Integer, List<Record<K2, V2>>>> entry : inteCache.entrySet()) {
            if (entry.getValue().get(number) != null) {
                data.addAll(entry.getValue().get(number));
            }
        }

        // sort todo: group by 怎么实现比较好?
        // Collections.sort(data);
        Map<K2, List<V2>> sortsData = new HashMap<>();
        for (Record<K2, V2> r : data) {
            if (sortsData.containsKey(r.getK())) {
                sortsData.get(r.getK()).add(r.getV());
            }
            else {
                List<V2> ls = new LinkedList<>();
                ls.add(r.getV());
                sortsData.put(r.getK(), ls);
            }
        }

        // reduce
        List<Record<K3,V3>> reduceRes = new LinkedList<>();
        for (Map.Entry<K2, List<V2>> entry : sortsData.entrySet()) {
            reduceRes.add(reducer.reduce(entry.getKey(), entry.getValue()));
        }

        // output to file
        DataFormat<K1, V1, K3, V3> output = outputFormat.getInstance();
        output.getRecordWriter().write(reduceRes);
        outputCache.put(number,output);
    }

    private <K,V> HashMap<K, List<V>> groupByKey(List<Record<K,V>> res)
    {
        HashMap<K,List<V>> groups = new HashMap<>();

        for(Record<K,V> r : res){
            if(groups.getOrDefault(r.getK(),null) == null){
                groups.put(r.getK(),new LinkedList<>(Collections.singletonList(r.getV())));
            } else {
                groups.get(r.getK()).add(r.getV());
            }
        }

        return groups;
    }

    /**
     * @param mapRes
     * @return
     */
    private <K,V> HashMap<Integer, List<Record<K, V>>> partition(List<Record<K, V>> mapRes)
    {
        HashMap<Integer, List<Record<K, V>>> tmpFiles = new HashMap<>(reduces);
        for (Record<K, V> r : mapRes) {
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
