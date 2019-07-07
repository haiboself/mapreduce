package dataformat;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@AllArgsConstructor
public class StringInputFormat implements DataFormat<Integer, String, String, String>
{
    private String content;
    private String delimiter;

    @Override
    public List<Partition<Integer, String>> partitions(int n)
    {
        assert n > 0;
        Objects.requireNonNull(content);

        String[] ss = content.split(delimiter);
        int step = 0;

        if(ss.length <= n){
            n = ss.length;
            step = 1;
        } else {
            step = ss.length / n;
        }

        List<Partition<Integer, String>> res = new ArrayList<>();

        int count = 0;
        for (int i = 0; i < n; i++) {

            List<Record<Integer, String>> part = new ArrayList<>(step);

            int end = step * i + step;
            if(i == n - 1 && end < ss.length){
                end = ss.length;
            }

            for (int s = step * i; s < end; s++) {
                part.add(new Record<>(count++, ss[s]));
            }

            res.add(part::iterator);
        }

        return res;
    }
}
