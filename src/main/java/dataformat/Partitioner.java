package dataformat;

public interface Partitioner
{
    public <K,V> int getPartition(K key, V value, int numPartitions);
}
