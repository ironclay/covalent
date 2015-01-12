package covalent.item;

/**
 * Used to dynamically determine the structure of an item.
 * 
 * @author Mario Ceste, Jr.
 * @since 1.0
 */
public interface ItemMetadataProvider<T> {

    /**
     * Determine the metadata for all items.
     * 
     * @return the item's metadata
     */
    ItemMetadata<T> get();

    /**
     * Copy from one item to another.
     * <p/>
     * This method is invoked to allow implementations to preserve data from another item.
     * 
     * @param input the item to copy from
     * @param output the item to modify
     * 
     * @see ItemMetadata#create() 
     */
    void copyFrom(T input, T output);

}
