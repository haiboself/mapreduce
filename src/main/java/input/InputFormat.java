package input;

import java.util.List;

/**
 * @author haibo
 */
public interface InputFormat<K1 extends Comparable<K1>,V1>
{
    public List<Partition<K1,V1>> partitions(int n);
}
