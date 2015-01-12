package covalent.item;

import java.io.Closeable;
import java.io.IOException;

/**
 * Interface for reading items. It's used to receive an item from a previous step.
 * 
 * @author Mario Ceste, Jr.
 * @since 1.0
 */
public interface ItemReader<T> extends Closeable {

    /**
     * Read an item and advance to the next one. This method will block indefinitely until an item is available.
     * <p/>
     * NOTE: Implementations must return {@code null} when the end of stream is reached.
     * 
     * @return an item, or {@code null} if no more items are available
     * 
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if interrupted while waiting
     */
    T read() throws IOException, InterruptedException;

}
