package demo;

import dataformat.OutputFormat;
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
 * 部分排序
 */
public class PartitionOrdering
{
    static class POrderMapper extends mapper.Mapper<Integer,String,Integer,Integer> {

        @Override
        public List<Record<Integer, Integer>> map(Integer integer, String s)
        {
            return Collections.singletonList(new Record<>(Integer.parseInt(s),Integer.parseInt(s)));
        }
    }

    static class POrderReducer extends reducer.Reducer<Integer,Integer,String,List<Integer>> {

        @Override
        public Record<String, List<Integer>> reduce(Integer k, List<Integer> vs)
        {
            return new Record<>("",vs);
        }
    }

    public static void main(String[] args)
    {
        Driver<Integer,String,Integer,Integer, String, List<Integer>> driver = new Driver<>();
        driver.setConf(new Conf());
        driver.setMapper(new POrderMapper());
        driver.setReducer(new POrderReducer());
        driver.setReduces(3);
        driver.setInputFormat(StringInputFormat.of("1,2,-1,4,4,3,2,1,10,-1,3,3,-2,10,-1,2,4,4,3,-1",","));
        driver.setOutputFormat(new StringOutputFormat<>());

        HashMap<Integer, OutputFormat<String,List<Integer>>> res = driver.submit();
        res.values().stream()
                .map(x -> ((StringOutputFormat<String,List<Integer>>)x).getContent())
                .map(Arrays::toString)
                .forEach(System.out::println);
    }
}
