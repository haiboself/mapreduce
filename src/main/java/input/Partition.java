package input;

import java.util.Iterator;

// todo: 如何更好实现
public interface Partition<K1 extends Comparable<K1>,V1>
{
    public Iterator<Record<K1,V1>> iterator();
}
