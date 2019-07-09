package dataformat;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import reducer.Reducer;
import schedule.Conf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@AllArgsConstructor
@NoArgsConstructor
public class StringInputFormat implements DataFormat<Integer, String, String, Integer>
{
    private String[] content;
    private final int blockSize = 2;

    public StringInputFormat(String str, String delimiter){
        content = str.split(delimiter);
    }

    @Override
    public List<Partition> partitions(Conf conf)
    {
        Objects.requireNonNull(content);

        int n;
        if (content.length % blockSize == 0){
            n = content.length / blockSize;
        } else {
            n = content.length / blockSize + 1;
        }

        List<Partition> res = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            int start = blockSize * i;
            int end = Math.min(blockSize * (i + 1), content.length);
            int size = Math.min(blockSize, content.length - start);

            res.add(new ArrayPartition(size,start,end));
        }

        return res;
    }

    @Override
    public RecordReader<Integer, String> getRecordReader(Partition p)
    {
        Objects.requireNonNull(p);

        return new RecordReader<Integer, String>() {
            int start = Integer.parseInt(p.getLocations()[0]);
            int end = Integer.parseInt(p.getLocations()[1]);
            int index = start - 1;

            @Override
            public boolean hasNext()
            {
                return ++index < end;
            }

            @Override
            public Integer getCurKey()
            {
                return index;
            }

            @Override
            public String getCurVal()
            {
                return content[index];
            }
        };
    }

    @Override
    public RecordWriter<String, Integer> getRecordWriter()
    {
        return res -> {
            content = new String[res.size()];
            int index = 0;
            for (Record<String,Integer> r : res){
                content[index++] = r.getK() + " " + r.getV();
            }
        };
    }

    @Override
    public DataFormat<Integer, String, String, Integer> getInstance()
    {
        return new StringInputFormat();
    }

    @Override
    public String toString(){
        return Arrays.toString(content);
    }
}


@AllArgsConstructor
@Getter
class ArrayPartition implements Partition{

    int size;
    int start;
    int end;

    @Override
    public long getLength()
    {
        return size;
    }

    @Override
    public String[] getLocations()
    {
        return new String[]{String.valueOf(start),String.valueOf(end)};
    }
}
