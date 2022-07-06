package rsm;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.UUID;

@Data
@EqualsAndHashCode(callSuper = false, of = "taskId")
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
public class ResTask {
    @Getter
    private String taskId = UUID.randomUUID().toString();

    public void init(){}
    public Output run(){return null;}

    public String desc(){
        return taskId;
    }
}
