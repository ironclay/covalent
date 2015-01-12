package covalent.item;

import java.io.IOException;

/**
 * Extension of the {@link ItemReader} interface that allows an item to be read without advancing to the next one.
 * 
 * @author Mario Ceste, Jr.
 * @since 1.0
 */
public interface PeekableItemReader<T> extends ItemReader<T> {

    /**
     * Read an item without advancing to the next one. This method will block indefinitely until an item is available.
     * <p/>
     * Consecutive calls to this method without invoking {@link #read()} will always return the same item.
     * 
     * @return an item, or {@code null} if no more items are available
     * 
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if interrupted while waiting
     * 
     * @see ItemReader#read() 
     */
    T peek() throws IOException, InterruptedException;

}
