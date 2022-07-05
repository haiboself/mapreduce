package core;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import core.dataformat.KvPair;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
public interface Reducer<K2, V2, K3, V3> {
    KvPair<K3, V3> reduce(K2 k2, Iterable<V2> v2s);
}
