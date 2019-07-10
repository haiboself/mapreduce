package dataformat;

/**
 * 输出数据源抽象
 * @param <KOUT>
 * @param <VOUT>
 */
public interface OutputFormat<KOUT, VOUT>
{
    public RecordWriter<KOUT, VOUT> getRecordWriter();
    @Override
    public String toString();
}