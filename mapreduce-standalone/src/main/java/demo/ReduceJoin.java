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

    static class LeftJoinReducer extends reducer.Reducer<String, Record<String,String>, String, List<String>>
    {
        @Override
        public Record<String, List<String>> reduce(String k, List<Record<String, String>> vs)
        {
            List<String> res = new LinkedList<>();
            List<String> t1s = vs.stream().filter(r -> "T1".equals(r.getK())).map(Record::getV).collect(Collectors.toList());

            if(t1s.size() == vs.size()){
                for(Record<String,String> r : vs){
                    res.add(r.getV() + " _ _");
                }
            } else {
                for (Record<String, String> r : vs) {
                    if ("T2".equals(r.getK())) {
                        for (String a : t1s) {
                            res.add(a + " " + r.getV());
                        }
                    }
                }
            }

            return new Record<>(k,res);
        }
    }

    static class RightJoinReducer extends reducer.Reducer<String, Record<String, String>, String, List<String>>
    {
        @Override
        public Record<String, List<String>> reduce(String k, List<Record<String, String>> vs)
        {
            List<String> res = new LinkedList<>();
            List<String> t1s = vs.stream().filter(r -> "T1".equals(r.getK())).map(Record::getV).collect(Collectors.toList());

            if(t1s.size() == 0){
                for(Record<String,String> r : vs){
                    res.add("_ _ " + r.getV());
                }
            } else {
                for (Record<String, String> r : vs) {
                    if ("T2".equals(r.getK())) {
                        for (String a : t1s) {
                            res.add(a + " " + r.getV());
                        }
                    }
                }
            }

            return new Record<>(k, res);
        }
    }

    public static void main(String[] args)
    {
        System.out.println("T1.id,T1.lower,T2.id,T2.upper join on T1.id = T2.id");
        innerJoin();

        System.out.println("\nT1.id,T1.lower,T2.id,T2.upper left join on T1.id = T2.id");
        leftJoin();

        System.out.println("\nT1.id,T1.lower,T2.id,T2.upper right join on T1.id = T2.id");
        rightJoin();
    }

    private static void innerJoin()
    {
        MultiInputDriver<Integer, String, String, Record<String,String>, String, List<String>> driver = new MultiInputDriver<>();
        driver.setConf(new Conf());

        /* explain process:
         *   T1     join     T2     on id = id
         * id val          id val
         * 6  f            1  A
         * 1  a            2  B
         * 1  a            3  C     = ?
         * 2  b            4  D
         * 4  d            5  E
         * 5  e            6  F
         *
         * Step1:
         * - data split -> data skew
         * - mapStage
         *  - run map task
         *  - spill to local disk -> data skew
         * - reduceStage
         *  - fetch map output file -> data skew
         *  - sort and merge
         *  - run reduce task
         */
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

    private static void leftJoin(){
        MultiInputDriver<Integer, String, String, Record<String,String>, String, List<String>> driver = new MultiInputDriver<>();
        driver.setConf(new Conf());

        driver.addMapper(new JoinAMapper(), StringInputFormat.of("6 f,1 a,1 a,2 b,4 d,5 e,0 h", ","));
        driver.addMapper(new JoinBMapper(), StringInputFormat.of("1 A,2 B,2 B,3 C,4 D,5 E,6 F", ","));

        driver.setReducer(new LeftJoinReducer());
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

    private static void rightJoin(){
        MultiInputDriver<Integer, String, String, Record<String,String>, String, List<String>> driver = new MultiInputDriver<>();
        driver.setConf(new Conf());

        driver.addMapper(new JoinAMapper(), StringInputFormat.of("6 f,1 a,1 a,2 b,4 d,5 e,0 h", ","));
        driver.addMapper(new JoinBMapper(), StringInputFormat.of("1 A,2 B,2 B,3 C,4 D,5 E,6 F", ","));

        driver.setReducer(new RightJoinReducer());
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

