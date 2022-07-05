package sample;

import com.typesafe.config.ConfigFactory;
import core.Driver;
import core.Mapper;
import core.Reducer;
import core.dataformat.KvPair;
import core.dataformat.LocalTextFileFormat;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

public class WordCount {

    static class TokenizerMapper implements Mapper<Integer, String, String, Integer>
    {
        @Override
        public Iterable<KvPair<String, Integer>> map(Integer seq, String word) {
            return Collections.singletonList(
                    new KvPair<>(word, 1)
            );
        }
    }

    static class IntSumReducer implements Reducer<String,Integer,String,Integer> {

        @Override
        public KvPair<String, Integer> reduce(String word, Iterable<Integer> wordCount) {
            int totalCount = 0;
            for (Integer integer : wordCount) {
                totalCount += integer;
            }

            return new KvPair<>(word, totalCount);
        }
    }

    public static void main(String[] args) throws IOException {
        // test data
        String path = "/tmp/test_input_files";
        FileUtils.cleanDirectory(new File(path));
        FileUtils.cleanDirectory(new File("/tmp/test_output_files"));
        FileUtils.writeLines(new File(path + "/1.txt"), Arrays.asList("a,b,c,d,e,f,a,a,a".split(",")));
        FileUtils.writeLines(new File(path + "/2.txt"), Arrays.asList("x,b,c,ddd,d".split(",")));
        FileUtils.writeLines(new File(path + "/3.txt"), Arrays.asList("a,x".split(",")));


        Driver<Integer, String, String, Integer, String, Integer> driver = new Driver<>();
        driver.setConf(ConfigFactory.parseMap(new HashMap<String, Object>(){{
            put("partitioner.class", "default");
        }}));

        driver.setMapper(new TokenizerMapper());
        driver.setInputFormat(new LocalTextFileFormat("/tmp/test_input_files"));

        driver.setReducer(new IntSumReducer());
        driver.setOutputFormat(new LocalTextFileFormat("/tmp/test_output_files"));
        driver.setReducerNum(1);

        driver.submit();
        new LocalTextFileFormat("/tmp/test_output_files").getSplits().forEach(split -> {
            split.iterator().forEachRemaining(pair -> System.out.println(pair.getV()));
        });
    }

}
