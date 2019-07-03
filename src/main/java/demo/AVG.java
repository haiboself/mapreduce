package demo;

import input.Record;
import input.StringInputFormat;
import schedule.Master;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
        Master<Integer,String,Integer,Integer> master = new Master<>();
        master.setMap(new AVG.Mapper());
        master.setReduce(new AVG.Reducer());

        master.setMaps(6);

        master.setInputFormat(new StringInputFormat("1,2,3,4,5,6,7,8,9,10",","));
        HashMap<Integer,List<Integer>> res = master.process();

        for(Map.Entry<Integer,List<Integer>> entry : res.entrySet()){
            System.out.println(entry.getValue().stream().map(String::valueOf).collect(Collectors.joining("\n")));
        }
    }
}
