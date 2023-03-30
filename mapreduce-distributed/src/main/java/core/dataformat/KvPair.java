package core.dataformat;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
public class KvPair <K,V> implements Serializable {

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
    private K k;

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
    private V v;
}
