package dataformat;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import schedule.Conf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 * @author haibo
 */
@NoArgsConstructor
public class StringInputFormat implements InputFormat<Integer,String>
{
    private String[] content;
    private final int blockSize = 2;

    private StringInputFormat(String[] content){
        this.content = content;
    }

    public static StringInputFormat of(String str, String delimiter){
        return new StringInputFormat(str.split(delimiter));
    }

    public static <K,V> InputFormat<Integer, String> of(Collection<OutputFormat<K, V>> data)
    {
        List<String> contents = new LinkedList<>();
        for(OutputFormat out : data){
            contents.addAll(Arrays.asList(((StringOutputFormat)out).getContent()));
        }

        return new StringInputFormat(contents.toArray(new String[0]));
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
