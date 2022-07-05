package core.dataformat;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
public interface DataFormat {
    List<Split> getSplits();

    <V3, K3> void append(String name, KvPair<K3,V3> res);
}
