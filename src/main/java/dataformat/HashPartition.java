package dataformat;

import java.util.Objects;

public class HashPartition<K2,V2> implements Partitioner<K2,V2>
{
    @Override
    public int getPartition(K2 key, V2 value, int numPartitions)
    {
        return Objects.hashCode(key) % numPartitions;
    }
}
