package mapper;

import dataformat.Record;

import java.util.List;

public abstract class Mapper<K1, V1, K2, V2>
{
    /**
     * TODO: 框架要保证 k1 和 v1 不是 null
     * @param k1
     * @param v1
     * @return
     */
    public abstract List<Record<K2,V2>> map(K1 k1, V1 v1);
}
