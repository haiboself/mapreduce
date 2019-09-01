package reducer;

import dataformat.Record;

import java.util.List;

public abstract class Reducer<K2,V2,K3,V3>{

    /**
     * TODO: 框架要保证 k1 和 vs 不是 null
     * @param k
     * @param vs
     * @return
     */
    public abstract Record<K3,V3> reduce(K2 k, List<V2> vs);
}
