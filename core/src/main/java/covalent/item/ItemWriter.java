package covalent.item;

import java.io.Closeable;
import java.io.IOException;

/**
 * Interface for writing items. It's used to send an item to another step.
 * 
 * @author Mario Ceste, Jr.
 * @since 1.0
 */
public interface ItemWriter<T> extends Closeable {

    /**
     * Write the given items.
     * 
     * @param items the items to write
     * 
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if interrupted while waiting
     */
    void write(T... items) throws IOException, InterruptedException;

}
