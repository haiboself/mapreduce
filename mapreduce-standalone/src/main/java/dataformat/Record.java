package dataformat;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Record<K,V>
{
    private K k;
    private V v;
}
