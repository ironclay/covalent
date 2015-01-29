package covalent.io;

import com.google.common.base.Preconditions;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Class for writing Java primitive types to a stream of bytes.
 * 
 * @author Mario Ceste, Jr.
 * @since 1.0
 */
public final class Output {

    /**
     * The output stream.
     */
    private final DataOutput out;

    /**
     * The byte array to use for copying bytes.
     */
    public final byte[] buffer;

    /**
     * Sole constructor.
     * 
     * @param out the underlying output stream
     * @param bufferSize the buffer size
     */
    public Output(DataOutput out, int bufferSize) {
        Preconditions.checkNotNull(out);
        Preconditions.checkArgument(bufferSize > 0);
        this.out = out;
        this.buffer = new byte[bufferSize];
    }

    /**
     * Write a {@code byte} to the output stream.
     * 
     * @param b the byte to be written
     * 
     * @throws IOException if an I/O error occurs
     */
    public void write(int b) throws IOException {
        out.write(b);
    }

    /**
     * Write all of the bytes to the output stream.
     * 
     * @param b the data to be written
     * 
     * @throws IOException if an I/O error occurs
     */
    public void write(byte b[]) throws IOException {
        out.write(b);
    }

    /**
     * Write some of the bytes to the output stream.
     * 
     * @param b the data to be written
     * @param off the offset within the data of the first byte
     * @param len the number of bytes to write
     * 
     * @throws IOException if an I/O error occurs
     */
    public void write(byte b[], int off, int len) throws IOException {
        out.write(b, off, len);
    }

    /**
     * Write a {@code boolean} to the output stream.
     * 
     * @param v the boolean to be written
     * 
     * @throws IOException if an I/O error occurs
     */
    public void writeBoolean(boolean v) throws IOException {
        out.writeBoolean(v);
    }

    /**
     * Write a {@code byte} to the output stream.
     * 
     * @param v the byte to be written
     * 
     * @throws IOException if an I/O error occurs
     */
    public void writeByte(int v) throws IOException {
        out.writeByte(v);
    }

    /**
     * Write a {@code char} to the output stream.
     * 
     * @param v the char to be written
     * 
     * @throws IOException if an I/O error occurs
     */
    public void writeChar(int v) throws IOException {
        out.writeChar(v);
    }

    /**
     * Write a {@code double} to the output stream.
     * 
     * @param v the double to be written
     * 
     * @throws IOException if an I/O error occurs
     */
    public void writeDouble(double v) throws IOException {
        out.writeDouble(v);
    }

    /**
     * Write a {@code float} to the output stream.
     * 
     * @param v the float to be written
     * 
     * @throws IOException if an I/O error occurs
     */
    public void writeFloat(float v) throws IOException {
        out.writeFloat(v);
    }

    /**
     * Write an {@code int} to the output stream.
     * 
     * @param v the int to be written
     * 
     * @throws IOException if an I/O error occurs
     */
    public void writeInt(int v) throws IOException {
        out.writeInt(v);
    }

    /**
     * Write a {@code long} to the output stream.
     * 
     * @param v the long to be written
     * 
     * @throws IOException if an I/O error occurs
     */
    public void writeLong(long v) throws IOException {
        out.writeLong(v);
    }

    /**
     * Write a {@code short} to the output stream.
     * 
     * @param v the short to be written
     * 
     * @throws IOException if an I/O error occurs
     */
    public void writeShort(int v) throws IOException {
        out.writeShort(v);
    }

    /**
     * Write all of the characters to the output stream using a modified UTF-8 format.
     * 
     * @param csq the character sequence to be written
     * 
     * @throws IOException if an I/O error occurs
     */
    public void writeUTF(CharSequence csq) throws IOException {
        int len = csq.length(); // # of characters
        writeInt(len);
        
        for (int off = 0; off < len; off++) {
            char c = csq.charAt(off);

            if (c != 0 && c <= 0x007f) {
                writeByte(c);
            } else if (c > 0x07ff) {
                // 1110xxxx 10xxxxxx 10xxxxxx
                writeByte(0xe0 | ((c >> 12) & 0x0f));
                writeByte(0x80 | ((c >> 6) & 0x3f));
                writeByte(0x80 | (c & 0x3f)); 
            } else {
                // 110xxxxx 10xxxxxx
                writeByte(0xc0 | ((c >> 6) & 0x1f));
                writeByte(0x80 | (c & 0x3f));
            }
        }
    }

}