package core;

import lombok.Getter;
import res.Output;

import java.io.File;

public class MapOutPut implements Output {

    // local file path
    @Getter
    private String[] tmpFiles;
    private MapOutPut(String[] tmpFiles){
        this.tmpFiles = tmpFiles;
    }


    public static MapOutPut fromFiles(File[] files){
        String[] tmpFiles = new String[files.length];

        for (int i = 0; i < files.length; i++) {
            tmpFiles[i] = files[i].getAbsolutePath();
        }

        return new MapOutPut(tmpFiles);
    }
}