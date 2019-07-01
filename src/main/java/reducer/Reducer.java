package reducer;

import java.util.List;

public interface Reducer<K2,V2>
{
    public List<V2> reduce(K2 k, List<V2> vs);
}
