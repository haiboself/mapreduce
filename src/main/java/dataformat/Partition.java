package dataformat;

import java.io.IOException;

public interface Partition
{
    /**
     * Get the size of the split, so that the input splits can be sorted by size.
     * @return the number of bytes in the split
     * @throws IOException
     * @throws InterruptedException
     */
    long getLength() throws IOException, InterruptedException;

    /**
     * Get the list of nodes by name where the data for the split would be local.
     * The locations do not need to be serialized.
     *
     * @return a new array of the node nodes.
     * @throws IOException
     * @throws InterruptedException
     */
    String[] getLocations() throws IOException, InterruptedException;
}
