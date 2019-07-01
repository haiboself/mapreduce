package mapper;

import input.Record;

import java.util.List;

public interface Mapper<K1 extends Comparable<K1>,V1>
{
    // todo: K2,V2 是否要放在类上进行约束
    public <K2 extends Comparable<K2>,V2> List<Record<K2,V2>> map(Record<K1,V1> r);
}
