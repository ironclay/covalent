package covalent.io;

import com.google.common.base.Preconditions;
import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;

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
