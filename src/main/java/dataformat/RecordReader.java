package dataformat;


public interface RecordReader<KIN,VIN>
{
    public boolean hasNext();
    public KIN getCurKey();
    public VIN getCurVal();
}
