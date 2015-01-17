package covalent.io;

import java.io.DataInput;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.commons.io.IOUtils;

/**
 * Class for reading Java primitive types from a stream of bytes.
 * 
 * @author Mario Ceste, Jr.
 * @since 1.0
 */
public final class Input extends InputStream implements DataInput {

    /**
     * The input stream.
     */
    private final DataInput in;

    /**
     * The buffer.
     */
    private byte[] buffer = new byte[0x1000];

    /**
     * Sole constructor.
     * 
     * @param in the underlying input stream
     */
    private Input(DataInput in) {
        this.in = in;
    }

    @Override
    public int read() throws IOException {
        return in.readByte() & 0xFF;
    }

    @Override
    public boolean readBoolean() throws IOException {
        return in.readBoolean();
    }

    @Override
    public byte readByte() throws IOException {
        return in.readByte();
    }

    @Override
    public char readChar() throws IOException {
        return in.readChar();
    }

    @Override
    public double readDouble() throws IOException {
        return in.readDouble();
    }

    @Override
    public float readFloat() throws IOException {
        return in.readFloat();
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        in.readFully(b);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        in.readFully(b, off, len);
    }

    @Override
    public int readInt() throws IOException {
        return in.readInt();
    }

    @Override
    @Deprecated
    public String readLine() throws IOException {
        return in.readLine();
    }

    @Override
    public long readLong() throws IOException {
        return in.readLong();
    }

    @Override
    public short readShort() throws IOException {
        return in.readShort();
    }

    @Override
    public String readUTF() throws IOException {
        return in.readUTF();
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return in.readUnsignedByte();
    }

    @Override
    public int readUnsignedShort() throws IOException {
        return in.readUnsignedShort();
    }

    @Override
    public int skipBytes(int n) throws IOException {
        return in.skipBytes(n);
    }

}
