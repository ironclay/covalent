package covalent.item;

import covalent.io.Input;
import covalent.io.Output;
import java.io.IOException;

/**
 * Used to serialize an item to and from a stream of bytes.
 * 
 * @author Mario Ceste, Jr.
 * @since 1.0
 */
public interface ItemSerializer<T> {
    
    /**
     * Serialize the given item to a stream of bytes.
     * 
     * @param output the Output to write to
     * @param item the item to serialize
     * 
     * @throws IOException if an I/O error occurs
     */
    void serialize(Output output, T item) throws IOException;
    
    /**
     * Deserialize an item from a stream of bytes.
     * 
     * @param input the Input to read from
     * 
     * @return the deserialized item
     * 
     * @throws IOException if an I/O error occurs
     */
    T deserialize(Input input) throws IOException;
    
}
