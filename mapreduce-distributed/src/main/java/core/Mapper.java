package core;


import com.fasterxml.jackson.annotation.JsonTypeInfo;
import core.dataformat.KvPair;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
public interface Mapper<K1, V1, K2, V2> {
    Iterable<KvPair<K2, V2>> map(K1 k1, V1 v1);
}
