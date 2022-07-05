package core.dataformat;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Iterator;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
public abstract class Split {
    public abstract <K1,V1> Iterator<KvPair<K1, V1>> iterator();
}
