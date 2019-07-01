package demo;

import com.sun.tools.javac.util.StringUtils;
import input.InputFormat;
import input.Record;
import input.StringInputFormat;
import schedule.Master;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class WordCount
{

    static class Mapper
            implements mapper.Mapper<Integer, String>
    {
        @Override
        public <K2 extends Comparable<K2>, V2> List<Record<K2, V2>> map(Record<Integer, String> r)
        {
            List<Record<K2, V2>> res = new ArrayList<>();
            // todo: 范型如何更好使用
            res.add((Record<K2, V2>) new Record<String, String>(r.getV(), "1"));
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
        // todo: StringInputFormat 切分数据有 bug
        Master<Integer, String, String, String> master = new Master<>();
        master.setMap(new Mapper());
        master.setReduce(new Reducer());
        master.setMaps(2);

        master.setInputFormat(new StringInputFormat("a,b,c,d,e,f,g,a,a,a,b,c,d,d,g",","));
        HashMap<Integer,List<String>> res = master.process();

        for(Map.Entry<Integer,List<String>> entry : res.entrySet()){
            System.out.println(entry.getValue().stream().map(String::valueOf).collect(Collectors.joining("\n")));
        }
    }
}
