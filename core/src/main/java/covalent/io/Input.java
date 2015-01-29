package covalent.io;

import com.google.common.base.Preconditions;
import java.io.DataInput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import org.apache.commons.lang3.StringUtils;

/**
 * Class for reading Java primitive types from a stream of bytes.
 * 
 * @author Mario Ceste, Jr.
 * @since 1.0
 */
public final class Input {

    /**
     * The input stream.
     */
    private final DataInput in;

    /**
     * The byte array to use for copying bytes.
     */
    public final byte[] buffer;

    /**
     * Sole constructor.
     * 
     * @param in the underlying input stream
     * @param bufferSize the buffer size
     */
    public Input(DataInput in, int bufferSize) {
        Preconditions.checkNotNull(in);
        Preconditions.checkArgument(bufferSize > 0);
        this.in = in;
        this.buffer = new byte[bufferSize];
    }

    /**
     * Read a {@code boolean} from the input stream.
     * 
     * @return a boolean flag
     * 
     * @throws IOException 
     */
    public boolean readBoolean() throws IOException {
        return in.readBoolean();
    }

    /**
     * Read a {@code byte} from the input stream.
     * 
     * @return an 8-bit signed integer
     * 
     * @throws IOException if an I/O error occurs
     */
    public byte readByte() throws IOException {
        return in.readByte();
    }

    /**
     * Read a {@code char} from the input stream.
     * 
     * @return a 16-bit Unicode character
     * 
     * @throws IOException if an I/O error occurs
     */
    public char readChar() throws IOException {
        return in.readChar();
    }

    /**
     * Read a {@code double} from the input stream.
     * 
     * @return a 64-bit floating point
     * 
     * @throws IOException if an I/O error occurs
     */
    public double readDouble() throws IOException {
        return in.readDouble();
    }

    /**
     * Read a {@code float} from the input stream.
     * 
     * @return a 32-bit floating point
     * 
     * @throws IOException if an I/O error occurs
     */
    public float readFloat() throws IOException {
        return in.readFloat();
    }

    /**
     * Read bytes from the input stream.
     * 
     * @param b the buffer into which the data is read
     * 
     * @throws IOException if an I/O error occurs
     */
    public void readFully(byte[] b) throws IOException {
        in.readFully(b);
    }

    /**
     * Read the given number of bytes from the input stream.
     * 
     * @param b the buffer into which the data is read
     * @param off the offset into the data
     * @param len the number of bytes to be read
     * 
     * @throws IOException if an I/O error occurs
     */
    public void readFully(byte[] b, int off, int len) throws IOException {
        in.readFully(b, off, len);
    }

    /**
     * Read an {@code int} from the input stream.
     * 
     * @return a 32-bit signed integer
     * 
     * @throws IOException if an I/O error occurs
     */
    public int readInt() throws IOException {
        return in.readInt();
    }

    /**
     * Read a {@code long} from the input stream.
     * 
     * @return a 64-bit signed integer
     * 
     * @throws IOException if an I/O error occurs
     */
    public long readLong() throws IOException {
        return in.readLong();
    }

    /**
     * Read a {@code short} from the input stream.
     * 
     * @return a 16-bit signed integer
     * 
     * @throws IOException if an I/O error occurs
     */
    public short readShort() throws IOException {
        return in.readShort();
    }

    /**
     * Read an unsigned {@code byte} from the input stream.
     * 
     * @return an 8-bit unsigned integer
     * 
     * @throws IOException if an I/O error occurs
     */
    public int readUnsignedByte() throws IOException {
        return in.readUnsignedByte();
    }

    /**
     * Read an unsigned {@code short} from the input stream.
     * 
     * @return a 16-bit unsigned integer
     * 
     * @throws IOException if an I/O error occurs
     */
    public int readUnsignedShort() throws IOException {
        return in.readUnsignedShort();
    }

    /**
     * Skip over the given number of bytes from the input stream.
     * 
     * @param n the number of bytes to skip
     * 
     * @return the number of bytes skipped
     * 
     * @throws IOException if an I/O error occurs
     */
    public int skipBytes(int n) throws IOException {
        return in.skipBytes(n);
    }

    /**
     * Read a {@link String} from the input stream that has been encoded using a modified UTF-8 format.
     * 
     * @return a String of characters
     * 
     * @throws IOException if an I/O error occurs
     */
    public String readUTF() throws IOException {
        int len = readInt(); // # of characters

        if (len != 0) {
            char[] array = new char[len];

            for (int i = 0; i < array.length; i++) {
                int b1 = readByte(), b2, b3;

                switch (b1 >> 4) {
                    case 0xc:
                    case 0xd: // 110xxxxx 10xxxxxx
                        b2 = readByte();

                        if ((b2 & 0xc0) == 0x80) {
                            array[i] = (char) (((b1 & 0x1f) << 6) | (b1 & 0x3f));
                        } else {
                            throw new UTFDataFormatException();
                        }

                        break;
                    case 0xe: // 1110xxxx 10xxxxxx 10xxxxxx
                        b2 = readByte();
                        b3 = readByte();

                        if ((b2 & 0xc0) == 0x80 && (b3 & 0xc0) == 0x80) {
                            array[i] = (char) (((b1 & 0x0f) << 12) | ((b2 & 0x3f) << 6) | (b3 & 0x3f));
                        } else {
                            throw new UTFDataFormatException();
                        }

                        break;
                    default:
                        if (b1 < 0) {
                            throw new UTFDataFormatException();
                        } else {
                            array[i] = (char) b1;
                        }
                }
            }

            return new String(array);
        }

        return StringUtils.EMPTY;
    }

}
