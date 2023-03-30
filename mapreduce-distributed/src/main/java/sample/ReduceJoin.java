package sample;

import com.typesafe.config.ConfigFactory;
import core.Driver;
import core.Mapper;
import core.Reducer;
import core.dataformat.KvPair;
import core.dataformat.LocalTextFileFormat;
import io.vavr.control.Try;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class ReduceJoin {
    static class JoinAMapper implements Mapper<Integer, String, String, KvPair<String,String>>
    {

        @Override
        public Iterable<KvPair<String, KvPair<String, String>>> map(Integer integer, String s) {
            String[] res = s.split("\\s");
            String id = res[0];

            return Collections.singletonList(new KvPair<>(id, new KvPair<>("T1", s)));
        }
    }

    static class JoinBMapper implements Mapper<Integer, String, String, KvPair<String,String>>
    {
        @Override
        public Iterable<KvPair<String, KvPair<String, String>>> map(Integer integer, String s) {
            String[] res = s.split("\\s");
            String id = res[0];

            return Collections.singletonList(new KvPair<>(id, new KvPair<>("T2",s)));
        }
    }

    static class JoinReducer implements Reducer<String, KvPair<String,String>, String, List<String>>
    {

        @Override
        public KvPair<String, List<String>> reduce(String s, Iterable<KvPair<String, String>> records) {
            List<String> res = new LinkedList<>();
            List<String> t1s = new LinkedList<>();
            records.forEach(r -> {
                if("T1".equals(r.getK())){
                    t1s.add(r.getV());
                }
            });

            for(KvPair<String,String> r : records){
                if("T2".equals(r.getK())){
                    for(String a : t1s){
                        res.add(a + " " + r.getV());
                    }
                }
            }

            return new KvPair<>(s,res);
        }
    }

    @SneakyThrows
    public static void main(String[] args) {
        Driver<Integer, String, String,  KvPair<String,String>, String, List<String>> driver = new Driver<>();
        driver.setConf(ConfigFactory.parseMap(new HashMap<String, Object>(){{
            put("partitioner.class", "default");
        }}));

        String inputPathA = "/tmp/test_input_files_join_t1";
        String inputPathB = "/tmp/test_input_files_join_t2";
        String outputPath = "/tmp/test_output_files_join_res";

        Try.run(() -> FileUtils.cleanDirectory(new File(inputPathA)));
        Try.run(() -> FileUtils.cleanDirectory(new File(inputPathB)));
        Try.run(() -> FileUtils.cleanDirectory(new File(outputPath)));

        FileUtils.writeLines(new File(inputPathA + "/1.txt"), Arrays.asList("6 f,1 a,1 a,2 b,4 d,5 e".split(",")));
        FileUtils.writeLines(new File(inputPathB + "/2.txt"), Arrays.asList("1 A,2 B,2 B,3 C,4 D,5 E,6 F".split(",")));

        driver.addMapper(new JoinAMapper(), new LocalTextFileFormat(inputPathA));
        driver.addMapper(new JoinBMapper(), new LocalTextFileFormat(inputPathB));
        driver.setReducer(new JoinReducer());
        driver.setOutputFormat(new LocalTextFileFormat(outputPath));
        driver.setReducerNum(1);
        driver.submit();

        new LocalTextFileFormat(outputPath).getSplits().forEach(split -> split.iterator().forEachRemaining(pair -> {
            String line = pair.getV().toString().split(":")[1]
                    .replace("[", "")
                    .replace("]", "")
                    .trim();

            if (!line.isEmpty()) {
                Arrays.stream(line.split(",")).map(String::trim).forEach(System.out::println);
            }
        }));
    }
}