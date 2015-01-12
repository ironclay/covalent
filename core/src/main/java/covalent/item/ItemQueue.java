package covalent.item;

/**
 * A queue for passing items between steps.
 * 
 * @author Mario Ceste, Jr.
 * @since 1.0
 */
public interface ItemQueue<T> {

    /**
     * Return the capacity of this queue. This is the maximum number of items that can be written without blocking.
     * 
     * @return the queue's capacity
     */
    int getCapacity();

    /**
     * Return the current size of this queue. This is number of items that can be read without blocking.
     * 
     * @return the queue's size
     */
    int getSize();

    /**
     * Return the total number of items that have been written to this queue.
     * 
     * @return the input count
     */
    long getInputCount();

    /**
     * Return the total number of items that have been read from this queue.
     * 
     * @return the output count
     */
    long getOutputCount();

    /**
     * Return the {@link ItemReader} used by the next step to read from this queue.
     * 
     * @return the reader
     */
    ItemReader<T> getReader();

    /**
     * Return the {@link ItemWriter} used by the previous step to write to this queue.
     * 
     * @return the writer
     */
    ItemWriter<T> getWriter();

}
