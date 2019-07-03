package mapper;

import input.Record;

import java.util.List;

public interface Mapper<K1 extends Comparable<K1>, V1, K2 extends Comparable<K2>, V2>
{
    // todo: K2,V2 是否要放在方法上进行约束
    public List<Record<K2, V2>> map(Record<K1, V1> r);
}
