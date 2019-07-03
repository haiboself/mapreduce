package demo;

import input.Record;
import input.StringInputFormat;
import schedule.Driver;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// todo: 目前将所有数据 shuffle 到一个 reduce 去计算,如何分治去实现
public class AVG
{
    static class Mapper
            implements mapper.Mapper<Integer, String, Integer, Integer>
    {

        @Override
        public List<Record<Integer, Integer>> map(Record<Integer, String> r)
        {
            return Collections.singletonList(new Record<>(1,Integer.parseInt(r.getV())));
        }
    }

    static class Reducer
            implements reducer.Reducer<Integer, Integer>
    {

        @Override
        public List<Integer> reduce(Integer k, List<Integer> vs)
        {
            int sum = 0;
            for (int i : vs){
                sum += i;
            }

            return Collections.singletonList(sum / vs.size());
        }
    }

    public static void main(String[] args)
    {
        Driver<Integer,String,Integer,Integer> driver = new Driver<>();
        driver.setMap(new AVG.Mapper());
        driver.setReduce(new AVG.Reducer());

        driver.setMaps(6);

        driver.setInputFormat(new StringInputFormat("1,2,3,4,5,6,7,8,9,10",","));
        HashMap<Integer,List<Integer>> res = driver.process();

        for(Map.Entry<Integer,List<Integer>> entry : res.entrySet()){
            System.out.println(entry.getValue().stream().map(String::valueOf).collect(Collectors.joining("\n")));
        }
    }
}
