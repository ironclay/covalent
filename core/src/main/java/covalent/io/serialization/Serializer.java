package covalent.io.serialization;

import covalent.io.Input;
import covalent.io.Output;
import java.io.IOException;

/**
 * A serializer is responsible for converting an object to and from a stream of bytes.
 * <p/>
 * Serialization is used to efficiently store or transmit data.
 * 
 * @author Mario Ceste, Jr.
 * @since 1.0
 */
public interface Serializer<T> {

    /**
     * Deserialize an object from a stream of bytes.
     * 
     * @param input the Input to read from
     * 
     * @return the deserialized value
     * 
     * @throws IOException if an I/O error occurs
     */
    T read(Input input) throws IOException;

    /**
     * Serialize the given object to a stream of bytes.
     * 
     * @param output the Output to write to
     * @param value the value to serialize
     * 
     * @throws IOException if an I/O error occurs
     */
    void write(Output output, T value) throws IOException;

}
