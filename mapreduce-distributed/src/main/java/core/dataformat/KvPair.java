package core.dataformat;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;

@Getter
@AllArgsConstructor
public class KvPair <K,V> implements Serializable {

    private K k;
    private V v;


    @Override
    public String toString() {
        return String.format("%s\u0001%s", String.valueOf(k), String.valueOf(v));
    }
}
