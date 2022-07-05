package core.dataformat;


import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

@AllArgsConstructor
public class LocalTextFileFormat implements DataFormat {

    @NonNull
    private String dirPath;

    @Override
    public List<Split> getSplits() {
        return FileUtils.listFiles(new File(dirPath), new String[]{"txt"}, false)
                .stream()
                .map(LocalFileSplit::new)
                .collect(Collectors.toList());
    }
}
