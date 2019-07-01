package input;

import lombok.AllArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@AllArgsConstructor
public class StringInputFormat implements InputFormat<Integer,String>
{
    private String content;
    private String delimiter;

    @Override
    public List<Partition<Integer, String>> partitions(int n)
    {
        Objects.requireNonNull(content);
        String[] ss = content.split(delimiter);

        // todo: 当无法整除时
        int step = ss.length / n;

        List<Partition<Integer,String>> res = new ArrayList<>();

        int count = 0;
        for(int i = 0; i < n; i++){

            List<Record<Integer,String>> part = new ArrayList<>(step);

            int end = Math.min(step * i + step, ss.length);
            for(int s = step * i; s < end; s++){
                part.add(new Record<>(count++,ss[s]));
            }

            res.add(part::iterator);
        }

        return res;
    }
}
