package covalent.io;

import com.google.common.base.Preconditions;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Class for writing Java primitive types to a stream of bytes.
 * 
 * @author Mario Ceste, Jr.
 * @since 1.0
 */
public final class Output extends OutputStream implements DataOutput {

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
    
    @Override
    public void write(int b) throws IOException {
        out.write(b);
    }
    
    @Override
    public void writeBoolean(boolean v) throws IOException {
        out.writeBoolean(v);
    }
    
    @Override
    public void writeByte(int v) throws IOException {
        out.writeByte(v);
    }
    
    @Override
    public void writeBytes(String s) throws IOException {
        out.writeBytes(s);
    }
    
    @Override
    public void writeChar(int v) throws IOException {
        out.writeChar(v);
    }
    
    @Override
    public void writeChars(String s) throws IOException {
        out.writeChars(s);
    }
    
    @Override
    public void writeDouble(double v) throws IOException {
        out.writeDouble(v);
    }
    
    @Override
    public void writeFloat(float v) throws IOException {
        out.writeFloat(v);
    }
    
    @Override
    public void writeInt(int v) throws IOException {
        out.writeInt(v);
    }
    
    @Override
    public void writeLong(long v) throws IOException {
        out.writeLong(v);
    }
    
    @Override
    public void writeShort(int v) throws IOException {
        out.writeShort(v);
    }
    
    @Override
    public void writeUTF(String s) throws IOException {
        out.writeUTF(s);
    }
    
}
