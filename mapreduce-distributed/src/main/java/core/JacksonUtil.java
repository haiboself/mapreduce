package core;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;

public class JacksonUtil {


    private static ObjectMapper mapper = new ObjectMapper();

    @SneakyThrows
    public static <T> T fromJsonString(String js, Class<T> cls){
        return mapper.readValue(js, cls);
    }

    @SneakyThrows
    public static String toJsonString(Object obj){
        return mapper.writeValueAsString(obj);
    }
}
