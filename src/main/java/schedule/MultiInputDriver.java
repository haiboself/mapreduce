package schedule;

import dataformat.HashPartition;
import dataformat.InputFormat;
import dataformat.OutputFormat;
import dataformat.Partition;
import dataformat.Partitioner;
import dataformat.Record;
import dataformat.RecordReader;
import lombok.Data;
import lombok.NonNull;
import mapper.Mapper;
import reducer.Reducer;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * 支持多输入
 * @author jilian
 * @date 2019-07-15 22:55
 */
@Data
public class MultiInputDriver <K1, V1, K2 extends Comparable<K2>, V2, K3, V3>
{
    private Conf conf;
    /**
     * reduce 数
     */
    private int reduces = 1;

    private List<Record<Mapper<K1, V1, K2, V2>,InputFormat<K1,V1>>> mappers = new LinkedList<>();
    private Reducer<K2, V2, K3, V3> reducer;
    private Reducer<K2, V2, K2, V2> combiner;
    private OutputFormat<K3,V3> outputFormat;
    private Partitioner partitioner = new HashPartition();

    /**
     * cache map outputs
     */
    private HashMap<Integer, HashMap<Integer, List<Record<K2,V2>>>> inteCache = new HashMap<>(reduces);
    private HashMap<Integer, OutputFormat<K3, V3>> outputCache = new HashMap<>(reduces);

    public void addMapper(@NonNull Mapper<K1, V1, K2, V2> mapper, @NonNull InputFormat<K1,V1> input){
        mappers.add(new Record<>(mapper,input));
    }

    public HashMap<Integer, OutputFormat<K3, V3>> submit()
    {
        Objects.requireNonNull(mappers);
        Objects.requireNonNull(reducer);
        Objects.requireNonNull(outputFormat);

        // map stage
        int r = 0;

        for(Record<Mapper<K1, V1, K2, V2>, InputFormat<K1, V1>> m : mappers){
            Mapper<K1, V1, K2, V2> mapper = m.getK();
            InputFormat<K1,V1> inputFormat = m.getV();

            for(Partition p : inputFormat.partitions(conf)){
                mapTask(r++, mapper, inputFormat.getRecordReader(p));
            }
        }

        // reduce stage
        for (int i = 0; i < reduces; i++) {
            reduceTask(i);
        }

        // 返回结果
        return outputCache;
    }

    private void mapTask(int number, Mapper<K1, V1, K2, V2> mapper, RecordReader<K1,V1> reader)
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

        // sort & group by
        Map<K2, List<V2>> sortsData = new TreeMap<>();
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
        try {
            OutputFormat<K3, V3> output = outputFormat.getClass().newInstance();
            output.getRecordWriter().write(reduceRes);
            outputCache.put(number,output);
        } catch (Exception e){
            throw new RuntimeException(e);
        }
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

