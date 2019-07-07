package dataformat;

public interface Partitioner<K2,V2>
{
    public int getPartition(K2 key, V2 value, int numPartitions);
}
