package covalent.io;

import com.google.common.base.Preconditions;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;

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
     * The UTF-8 encoder.
     */
    private final CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder();

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
     * Write all of the characters to the output stream.
     * 
     * @param csq the text to be written
     * 
     * @throws IOException if an I/O error occurs
     */
    public void writeString(CharSequence csq) throws IOException {
        writeString(csq, 0, csq.length());
    }

    /**
     * Write some of the characters to the output stream.
     * 
     * @param csq the text to be written
     * @param start the index of the first character
     * @param end the index of the character following the last character
     * 
     * @throws IOException if an I/O error occurs
     */
    public void writeString(CharSequence csq, int start, int end) throws IOException {
        Preconditions.checkPositionIndexes(start, end, csq.length());
        writeUTF8(csq, start, end);
    }

    /**
     * Encode all of the characters using the UTF-8 character encoding and write the bytes to the output stream. 
     * 
     * @param in the input character buffer
     * @param out the output byte buffer
     * 
     * @throws IOException if an I/O error occurs
     */
    private void writeUTF8(CharSequence csq, int start, int end) throws IOException {
        long length = 0;

        // count the total number of bytes.
        for (int i = start, ch; i < end; i++) {
            ch = csq.charAt(i);

            if (ch >= 0x0000 && ch <= 0x007F) {
                length += 1;
            } else if (ch >= 0x0080 && ch <= 0x07FF) {
                length += 2;
            } else if (ch >= 0x0800 && ch <= 0xFFFF) {
                length += 3;
            } else {
                length += 4;
            }
        }

        writeInt(end - start);
        writeLong(length);
        
        // buffers
        CharBuffer cb = CharBuffer.wrap(csq, start, end);
        ByteBuffer bb = ByteBuffer.wrap(buffer);

        for (encoder.reset(); cb.hasRemaining();) {
            if (encoder.encode(cb, bb, true).isError() != true) {
                bb.flip();
                write(bb.array(), bb.arrayOffset() + bb.position(), bb.remaining());
                bb.clear();
            }
        }
    }

}
