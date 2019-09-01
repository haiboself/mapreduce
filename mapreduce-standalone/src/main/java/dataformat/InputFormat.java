package dataformat;

import schedule.Conf;

import java.util.List;

/**
 * 输入数据源抽象
 * @param <KIN>
 * @param <VIN>
 */
public interface InputFormat<KIN,VIN>
{
    /**
     * 由具体的输入源来确定分片方式和 map 数量
     * @return
     */
    public List<Partition> partitions(Conf conf);

    public RecordReader<KIN,VIN> getRecordReader(Partition p);
}
