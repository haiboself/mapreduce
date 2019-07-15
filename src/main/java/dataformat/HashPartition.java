package dataformat;

import java.util.Objects;

public class HashPartition<K,V> implements Partitioner<K,V>
{
    @Override
    public int getPartition(K key, V value, int numPartitions)
    {
        return Math.abs(Objects.hashCode(key) % numPartitions);
    }
}
