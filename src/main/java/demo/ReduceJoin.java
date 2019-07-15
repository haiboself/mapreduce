package demo;

import dataformat.OutputFormat;
import dataformat.Record;
import dataformat.StringInputFormat;
import dataformat.StringOutputFormat;
import schedule.Conf;
import schedule.MultiInputDriver;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author jilian
 * @date 2019-07-15 22:45
 */
public class ReduceJoin
{

    static class JoinAMapper extends mapper.Mapper<Integer, String, String, Record<String,String>>
    {
        @Override
        public List<Record<String, Record<String, String>>> map(Integer integer, String s)
        {
            String[] res = s.split("\\s");
            String id = res[0];

            return Collections.singletonList(new Record<>(id, new Record<>("T1", s)));
        }
    }

    static class JoinBMapper extends mapper.Mapper<Integer, String, String, Record<String,String>>
    {
        @Override
        public List<Record<String, Record<String, String>>> map(Integer integer, String s)
        {
            String[] res = s.split("\\s");
            String id = res[0];

            return Collections.singletonList(new Record<>(id, new Record<>("T2",s)));
        }
    }

    static class JoinReducer extends reducer.Reducer<String, Record<String,String>, String, List<String>>
    {
        @Override
        public Record<String, List<String>> reduce(String k, List<Record<String, String>> vs)
        {
            List<String> res = new LinkedList<>();
            List<String> t1s = vs.stream().filter(r -> "T1".equals(r.getK())).map(Record::getV).collect(Collectors.toList());

            for(Record<String,String> r : vs){
                if("T2".equals(r.getK())){
                    for(String a : t1s){
                        res.add(a + " " + r.getV());
                    }
                }
            }

            return new Record<>(k,res);
        }
    }

    public static void main(String[] args)
    {
        join();
    }

    private static void join()
    {
        MultiInputDriver<Integer, String, String, Record<String,String>, String, List<String>> driver = new MultiInputDriver<>();
        driver.setConf(new Conf());

        driver.addMapper(new JoinAMapper(), StringInputFormat.of("6 f,1 a,1 a,2 b,4 d,5 e", ","));
        driver.addMapper(new JoinBMapper(), StringInputFormat.of("1 A,2 B,2 B,3 C,4 D,5 E,6 F", ","));

        driver.setReducer(new JoinReducer());
        driver.setReduces(2);
        driver.setOutputFormat(new StringOutputFormat<>());

        HashMap<Integer, OutputFormat<String, List<String>>> res = driver.submit();

        System.out.println(res.values().stream()
                .map(o -> ((StringOutputFormat)o).getContent())
                .map(c -> Arrays.stream(c).filter(x -> x.length() > 2).map(x -> Arrays.stream(x.split(",")).map(String::trim).collect(Collectors.joining("\n"))))
                .map(c -> c.map(x -> x.replace("[","").replace("]","").trim()))
                .map(c -> c.collect(Collectors.joining("\n")))
                .collect(Collectors.joining("\n")));
    }
}
