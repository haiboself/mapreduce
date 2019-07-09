package dataformat;

import java.util.Objects;

public class HashPartition implements Partitioner
{
    @Override
    public <K,V> int getPartition(K key, V value, int numPartitions)
    {
        return Objects.hashCode(key) % numPartitions;
    }
}
