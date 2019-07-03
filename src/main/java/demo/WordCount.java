package demo;

import input.Record;
import input.StringInputFormat;
import schedule.Driver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class WordCount
{

    static class Mapper
            implements mapper.Mapper<Integer, String, String, String>
    {
        @Override
        public List<Record<String, String>> map(Record<Integer, String> r)
        {
            List<Record<String,String>> res = new ArrayList<>();
            res.add(new Record<>(r.getV(), "1"));
            return res;
        }
    }

    static class Reducer implements reducer.Reducer<String,String> {

        @Override
        public List<String> reduce(String k, List<String> vs)
        {
            return Collections.singletonList(k + " " + vs.size() + "\n");
        }
    }

    public static void main(String[] args)
    {
        Driver<Integer, String, String, String> driver = new Driver<>();
        driver.setMap(new Mapper());
        driver.setReduce(new Reducer());
        driver.setMaps(26);
        driver.setReduces(6);

        driver.setInputFormat(new StringInputFormat("a,b,c,d,e,f,g,a,a,a,b,c,d,d,g",","));
        HashMap<Integer,List<String>> res = driver.process();

        for(Map.Entry<Integer,List<String>> entry : res.entrySet()){
            System.out.println(entry.getValue().stream().map(String::valueOf).collect(Collectors.joining("\n")));
        }
    }
}
