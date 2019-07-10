package dataformat;

import schedule.Conf;

import java.util.List;

/**
 * 数据源抽象
 * @param <KIN>
 * @param <VIN>
 * @param <KOUT>
 * @param <VOUT>
 */
public interface DataFormat<KIN,VIN,KOUT,VOUT>
{
    /**
     * 由具体的输入源来确定分片方式和 map 数量
     * @return
     */
    public List<Partition> partitions(Conf conf);

    public RecordReader<KIN,VIN> getRecordReader(Partition p);

    public RecordWriter<KOUT,VOUT> getRecordWriter();
}
