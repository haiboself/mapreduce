package dataformat;

public interface Partitioner<K,V>
{
    public int getPartition(K key, V value, int numPartitions);
}
