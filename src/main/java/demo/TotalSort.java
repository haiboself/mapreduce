package demo;

import dataformat.OutputFormat;
import dataformat.Partitioner;
import dataformat.Record;
import dataformat.StringInputFormat;
import dataformat.StringOutputFormat;
import schedule.Conf;
import schedule.Driver;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * 全排序
 * @author jilian
 * @date 2019-07-15 22:03
 */
public class TotalSort
{
    static class TSortMapper extends mapper.Mapper<Integer,String,Integer,Integer> {

        @Override
        public List<Record<Integer, Integer>> map(Integer integer, String s)
        {
            return Collections.singletonList(new Record<>(Integer.parseInt(s),Integer.parseInt(s)));
        }
    }

    static class TSortReducer extends reducer.Reducer<Integer,Integer,String,List<Integer>> {

        @Override
        public Record<String, List<Integer>> reduce(Integer k, List<Integer> vs)
        {
            return new Record<>("",vs);
        }
    }

    static class NumberPartitioner implements Partitioner<Integer,Integer> {

        @Override
        public int getPartition(Integer k, Integer v, int numPartitions)
        {
            if(k <= 0){
                return Math.min(0,numPartitions);
            } else if(k <= 3){
                return Math.min(1,numPartitions);
            } else if(k <= 6){
                return Math.min(2,numPartitions);
            } else if(k <= 10){
                return Math.min(3,numPartitions);
            } else {
                return Math.min(4,numPartitions);
            }
        }
    }

    public static void main(String[] args)
    {
        Driver<Integer,String,Integer,Integer, String, List<Integer>> driver = new Driver<>();
        driver.setConf(new Conf());
        driver.setMapper(new TSortMapper());
        driver.setReducer(new TSortReducer());
        driver.setReduces(10);
        driver.setPartitioner(new NumberPartitioner());
        driver.setInputFormat(StringInputFormat.of("1,2,-1,4,4,3,2,1,10,-1,3,3,-2,10,-1,2,4,4,3,-1",","));
        driver.setOutputFormat(new StringOutputFormat<>());

        HashMap<Integer, OutputFormat<String,List<Integer>>> res = driver.submit();
        res.values().stream()
                .map(x -> ((StringOutputFormat<String,List<Integer>>)x).getContent())
                .map(Arrays::toString)
                .forEach(System.out::println);
    }
}

