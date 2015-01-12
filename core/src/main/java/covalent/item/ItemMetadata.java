package covalent.item;

/**
 * Describes the structure of an item.
 * 
 * @author Mario Ceste, Jr.
 * @since 1.0
 */
public interface ItemMetadata<T> {

    /**
     * Create an empty item.
     * 
     * @return the item
     */
    T create();

    /**
     * Return a copy of the given item. Immutable items don't need to be copied.
     * <p/>
     * An item isn't required to be thread-safe therefore it must be copied in order to be distributed.
     * 
     * @param item the item to copy
     * 
     * @return the copied item
     */
    T copy(T item);

}
