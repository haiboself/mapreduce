package dataformat;

import java.util.List;

public interface RecordWriter<KOUT,VOUT>
{
    void write(List<Record<KOUT,VOUT>> res);
}
