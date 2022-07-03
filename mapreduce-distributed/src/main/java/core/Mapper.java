package core;


import io.vavr.Tuple2;

public interface Mapper<K1, V1, K2, V2> {
    Iterable<Tuple2<K2, V2>> map(K1 k1, V1 v1);
}
