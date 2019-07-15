package dataformat;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author jilian
 * @date 2019-07-15 23:15
 */
@Data
@AllArgsConstructor
public class TextPair<F extends Comparable<F>,S extends Comparable<S>> implements Comparable<TextPair<F,S>>
{
    private F f;
    private S s;

    @Override
    public int compareTo(TextPair<F, S> o)
    {
        return f.compareTo(o.getF());
    }
}

