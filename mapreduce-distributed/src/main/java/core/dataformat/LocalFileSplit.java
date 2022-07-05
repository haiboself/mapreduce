package core.dataformat;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.io.File;
import java.util.Iterator;

@NoArgsConstructor
@AllArgsConstructor
public class LocalFileSplit extends Split {

    @NonNull
    private File file;

    @Override
    public <V1, K1> Iterator<KvPair<K1, V1>> iterator() {
        return null;
    }
}
