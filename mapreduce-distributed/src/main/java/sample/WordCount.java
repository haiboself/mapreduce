package sample;

import com.typesafe.config.ConfigFactory;
import core.Driver;
import core.Mapper;
import core.Reducer;
import core.dataformat.KvPair;
import core.dataformat.LocalTextFileFormat;

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

    public static void main(String[] args)
    {
        Driver<Integer, String, String, Integer, String, Integer> driver = new Driver<>();
        driver.setConf(ConfigFactory.parseMap(new HashMap<String, Object>(){{
            put("partitioner.class", "default");
        }}));

        driver.setMapper(new TokenizerMapper());
        driver.setInputFormat(new LocalTextFileFormat("/tmp/test_input_files"));

        driver.setReducer(new IntSumReducer());
        driver.setOutputFormat(new LocalTextFileFormat("/tmp/test_output_files"));
        driver.setReducerNum(10);

        driver.submit();
        new LocalTextFileFormat("/tmp/test_output_files").getSplits().forEach(split -> {
            split.iterator().forEachRemaining(System.out::println);
        });
    }

}
