package demo;

import dataformat.DataFormat;
import dataformat.Record;
import dataformat.StringInputFormat;
import schedule.Conf;
import schedule.Driver;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
            return Collections.singletonList(new Record<>(v, 1));
        }
    }

    static class IntSumReducer extends reducer.Reducer<String,Integer,String,Integer> {
        @Override
        public Record<String, Integer> reduce(String k, List<Integer> vs)
        {
            return new Record<>(k,vs.stream().reduce(0,(a,b) -> a + b));
        }
    }

    public static void main(String[] args)
    {
        Driver<Integer, String, String, Integer, String, Integer> driver = new Driver<>();
        driver.setConf(new Conf());
        driver.setMapper(new TokenizerMapper());
        driver.setReducer(new IntSumReducer());
        driver.setCombiner(new IntSumReducer());
        driver.setReduces(3);
        driver.setInputFormat(new StringInputFormat("a,b,c,d,e,f,a,a,a,a,g,a,a,a,b,c,d,d,g,af,f",","));
        driver.setOutputFormat(new StringInputFormat());

        HashMap<Integer,DataFormat<Integer,String,String,Integer>> res = driver.submit();
        for (DataFormat<Integer,String,String,Integer> f : res.values()){
            System.out.println(f);
        }
    }
}
