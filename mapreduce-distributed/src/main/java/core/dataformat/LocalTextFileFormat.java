package core.dataformat;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class LocalTextFileFormat implements DataFormat {

    @NonNull
    private String dirPath;

    @JsonCreator
    public LocalTextFileFormat(String dirPath){
        this.dirPath = dirPath;
    }

    @Override
    @JsonIgnore
    public List<Split> getSplits() {
        return FileUtils.listFiles(new File(dirPath), new String[]{"txt", "data"}, false)
                .stream()
                .map(LocalFileSplit::new)
                .collect(Collectors.toList());
    }

    @SneakyThrows
    public <V3, K3> void append(String name, KvPair<K3, V3> res) {
        FileUtils.writeLines(new File(dirPath + "/" + name + ".data"), Collections.singletonList(res.getK() + " : " + res.getV()), true);
    }
}
