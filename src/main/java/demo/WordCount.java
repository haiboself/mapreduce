package demo;

import dataformat.Record;
import dataformat.StringInputFormat;
import schedule.Conf;
import schedule.Driver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author haibo
 */
public class WordCount
{

    static class TokenizerMapper extends mapper.Mapper<Integer, String, String, Integer>
    {

        @Override
        public List<Record<String, Integer>> map(Integer k, String v)
        {
            List<Record<String,Integer>> res = new ArrayList<>();
            res.add(new Record<>(v, 1));
            return res;
        }
    }

    static class IntSumReducer extends reducer.Reducer<String,Integer,String,Integer> {
        @Override
        public Record<String, Integer> reduce(String k, List<Integer> vs)
        {
            return new Record<>(k,vs.size());
        }
    }

    public static void main(String[] args)
    {
        Driver<Integer, String, String, Integer, String, Integer> driver = new Driver<>();
        driver.setConf(new Conf());
        driver.setMapper(new TokenizerMapper());
        driver.setReducer(new IntSumReducer());
        driver.setCombiner(new IntSumReducer());
        driver.setReduces(6);
        driver.setInputFormat(new StringInputFormat("a,b,c,d,e,f,g,a,a,a,b,c,d,d,g",","));
        driver.setOutputFormat(new StringInputFormat());

        HashMap<Integer,List<String>> res = driver.process();

        for(Map.Entry<Integer,List<String>> entry : res.entrySet()){
            System.out.println(entry.getValue().stream().map(String::valueOf).collect(Collectors.joining("\n")));
        }
    }
}
