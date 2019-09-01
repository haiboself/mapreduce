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

public class Sum
{

    static class SumMapper extends mapper.Mapper<Integer, String, String, Double>
    {
        @Override
        public List<Record<String, Double>> map(Integer k, String v)
        {
            return Collections.singletonList(new Record<>("sum part",Double.parseDouble(v)));
        }
    }

    static class SumReducer extends reducer.Reducer<String,Double,String,Double> {
        @Override
        public Record<String, Double> reduce(String k, List<Double> vs)
        {
            return new Record<>("sum",vs.stream().reduce(0D,(a,b) -> a + b));
        }
    }

    public static void main(String[] args)
    {
        List<StringInputFormat> datas = Arrays.asList(
                StringInputFormat.of("1,2,3,4,5,6,7,8,9,10", ","),
                StringInputFormat.of("1,2,3,4,5,6,7,8,9,11", ","),
                StringInputFormat.of("1,2,3,4,5,6,7,8,9,12", ","),
                StringInputFormat.of("1,2,3,4,5,6,7,8,9,13", ",")
        );

        System.out.println("sum: " + sum(datas));
    }

    private static double sum(List<StringInputFormat> datas){
        if(datas.size() == 1){
            return sum(datas.get(0));
        } else {
            int mid = datas.size() / 2;
            return sum(datas.subList(0,mid)) + sum(datas.subList(mid,datas.size()));
        }
    }

    private static double sum(StringInputFormat inputFormat)
    {
        Driver<Integer,String,String,Double, String, Double> driver = new Driver<>();
        driver.setConf(new Conf());
        driver.setMapper(new Sum.SumMapper());
        driver.setReducer(new Sum.SumReducer());
        driver.setCombiner(new Sum.SumReducer());
        driver.setReduces(1);
        driver.setInputFormat(inputFormat);
        driver.setOutputFormat(new StringOutputFormat<>());

        HashMap<Integer, OutputFormat<String,Double>> res = driver.submit();
        return Double.parseDouble(((StringOutputFormat)res.get(0)).getContent()[0]);
    }
}
