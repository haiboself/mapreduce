package core;

import io.vavr.Tuple2;

public interface Reducer<K2, V2, K3, V3> {
    Tuple2<K3, V3> reduce(K2 k2, Iterable<V2> v2s);
}
