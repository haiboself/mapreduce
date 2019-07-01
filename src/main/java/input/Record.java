package input;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Record<K extends Comparable<K>,V> implements Comparable<Record<K,V>>
{
    private K k;
    private V v;

    @Override
    public int compareTo(Record<K, V> o)
    {
        return k.compareTo(o.k);
    }
}
