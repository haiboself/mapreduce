package schedule;

import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

@NoArgsConstructor
public class Conf implements Iterable<Map.Entry<String,String>>
{

    /**
     * 只在 driver 做 put 操作,所以不考虑并发
     */
    private HashMap<String,String> properties = new HashMap<>();

    public void set(@NonNull String k, @NonNull String v){
        properties.put(k,v);
    }

    public String get(@NonNull String k){
        return properties.getOrDefault(k,null);
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
        // Get a copy of just the string to string pairs. After the old object
        // methods that allow non-strings to be put into configurations are removed,
        // we could replace properties with a Map<String,String> and get rid of this
        // code.
        Map<String,String> result = new HashMap<>();
        for(Map.Entry<String,String> item: properties.entrySet()) {
            if (item.getKey() != null &&
                    item.getValue() != null) {
                result.put(item.getKey(), item.getValue());
            }
        }
        return result.entrySet().iterator();
    }
}
