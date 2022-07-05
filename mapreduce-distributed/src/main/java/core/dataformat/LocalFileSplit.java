package core.dataformat;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@NoArgsConstructor
@AllArgsConstructor
public class LocalFileSplit extends Split {

    @NonNull
    private File file;

    @Override
    @SneakyThrows
    public <K1,V1> Iterator<KvPair<K1, V1>> iterator() {
        List<KvPair<K1, V1>> res = new LinkedList<>();
        AtomicInteger index = new AtomicInteger();
        FileUtils.readLines(file).forEach(line -> {
            res.add(new KvPair(index.getAndIncrement(), line));
        });

        return res.iterator();
    }
}
