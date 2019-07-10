package demo;

import dataformat.OutputFormat;
import dataformat.Record;
import dataformat.StringInputFormat;
import dataformat.StringOutputFormat;
import schedule.Conf;
import schedule.Driver;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * @author jilian
 * @date 2019/7/10 13:58
 */
public class Avg
{

    static class AvgMapper extends mapper.Mapper<Integer, String, String, Double>
    {
        @Override
        public List<Record<String, Double>> map(Integer k, String v)
        {
            return Collections.singletonList(new Record<>("avg",Double.parseDouble(v)));
        }
    }

    static class AvgReducer extends reducer.Reducer<String,Double,String,Double> {
        @Override
        public Record<String, Double> reduce(String k, List<Double> vs)
        {
            return new Record<>(k,vs.stream().reduce(0D,(a,b) -> a + b) / vs.size());
        }
    }

    public static void main(String[] args)
    {
        Driver<Integer,String,String,Double, String, Double> driver = new Driver<>();
        driver.setConf(new Conf());
        driver.setMapper(new AvgMapper());
        driver.setReducer(new AvgReducer());
        driver.setCombiner(new AvgReducer());
        driver.setReduces(1);
        driver.setInputFormat(StringInputFormat.of("1,2,3,4,5,6,7,8,9,10",","));
        driver.setOutputFormat(new StringOutputFormat<>());

        HashMap<Integer, OutputFormat<String,Double>> res = driver.submit();
        for (OutputFormat<String,Double> f : res.values()){
            System.out.println(f);
        }
    }
}
