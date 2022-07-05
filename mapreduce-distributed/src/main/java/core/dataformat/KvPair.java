package core.dataformat;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Getter
@AllArgsConstructor
@NoArgsConstructor
public class KvPair <K,V> implements Serializable {
    private K k;
    private V v;
}
