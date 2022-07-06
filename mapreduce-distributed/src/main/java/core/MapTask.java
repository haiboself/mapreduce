package core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.typesafe.config.Config;
import core.dataformat.KvPair;
import core.dataformat.Partitioner;
import core.dataformat.Split;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import rsm.ResTask;

import java.io.File;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class MapTask<K1, V1, K2, V2> extends ResTask {

    private Split split;
    private Mapper<K1, V1, K2, V2> mapper;
    private Partitioner partitioner;
    private int reduceNumns;
    private Config config;

    private File[] spillFiles;

    @JsonCreator
    public MapTask(Mapper<K1, V1, K2, V2> mapper, Split split, int reduceNumns) {
        this.mapper = mapper;
        this.split = split;
        this.reduceNumns = reduceNumns;
    }


    @Override
    public void init() {
        this.spillFiles = new File[this.reduceNumns];
        for (int i = 0; i < this.spillFiles.length; i++) {
            this.spillFiles[i] = new File("/tmp/maptask/" + this.getTaskId() + "/" + i + ".txt");
        }
        this.partitioner = new Partitioner() {
            @Override
            public <K, V> int getPartition(K key, V value, int numPartitions) {
                return Math.abs(Objects.hashCode(key) % numPartitions);
            }
        };
    }

    @Override
    @SneakyThrows
    public MapOutPut run() {
        init();
        List<Iterable<KvPair<K2, V2>> > middleOutputs = new LinkedList<>();
        Iterator<KvPair<K1,V1>> iterator = split.iterator();
        iterator.forEachRemaining(kvInput ->
                middleOutputs.add(mapper.map(kvInput.getK(), kvInput.getV()))
        );

        middleOutputs.forEach(output -> output.forEach(this::partitionForShuffle));

        TimeUnit.SECONDS.sleep(3);
        return MapOutPut.fromFiles(spillFiles);
    }

    @SneakyThrows
    private void partitionForShuffle(KvPair<K2,V2> kvPair) {
        Objects.requireNonNull(kvPair);

        int partitionNum = partitioner.getPartition(kvPair.getK(), kvPair.getV(), reduceNumns);
        FileUtils.writeLines(
                spillFiles[partitionNum], Collections.singleton(JacksonUtil.toJsonString(kvPair)), true);
    }

    @Override
    public String desc() {
        return "mapTask " + getTaskId();
    }
}
