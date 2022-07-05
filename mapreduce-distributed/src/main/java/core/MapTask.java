package core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.typesafe.config.Config;
import core.dataformat.KvPair;
import core.dataformat.Partitioner;
import core.dataformat.Split;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import res.ResTask;

import java.io.File;
import java.util.*;

public class MapTask<K1, V1, K2, V2> extends ResTask {

    private Split split;
    private Mapper<K1, V1, K2, V2> mapper;
    private Partitioner partitioner;
    private int reduceNumns;
    private Config config;

    private File[] spillFiles;

    @JsonCreator
    public MapTask(Mapper<K1, V1, K2, V2> mapper, Split split) {
        this.mapper = mapper;
        this.split = split;
    }


    @Override
    public void init() {
    }

    @Override
    public MapOutPut run() {
        List<Iterable<KvPair<K2, V2>> > middleOutputs = new LinkedList<>();
        Iterator<KvPair<K1,V1>> iterator = split.iterator();
        iterator.forEachRemaining(kvInput ->
                middleOutputs.add(mapper.map(kvInput.getK(), kvInput.getV()))
        );

        middleOutputs.forEach(output -> output.forEach(this::partitionForShuffle));

        return MapOutPut.fromFiles(spillFiles);
    }

    @SneakyThrows
    private void partitionForShuffle(KvPair<K2,V2> kvPair) {
        Objects.requireNonNull(kvPair);

        int partitionNum = partitioner.getPartition(kvPair.getK(), kvPair.getV(), reduceNumns);
        FileUtils.writeLines(
                spillFiles[partitionNum], Collections.singleton(kvPair.toString()), true
        );
    }
}
