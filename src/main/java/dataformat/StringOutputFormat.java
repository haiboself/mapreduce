package dataformat;

import lombok.Getter;

public class StringOutputFormat<KOUT,VOUT> implements OutputFormat<KOUT,VOUT>
{
    @Getter
    private String[] content;
    private String[] ks;

    @Override
    public RecordWriter<KOUT, VOUT> getRecordWriter()
    {
        return res -> {
            content = new String[res.size()];
            ks = new String[res.size()];
            int index = 0;
            for (Record<KOUT,VOUT> r : res){
                content[index] = r.getV().toString();
                ks[index] = r.getK().toString();

                index++;
            }
        };
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder("[");
        for(int i = 0; i < content.length; i++){
            sb.append(ks[i]).append(": ").append(content[i]);
            if(i < content.length - 1){
                sb.append(",    ");
            }
        }

        sb.append("]");
        return sb.toString();
    }
}
