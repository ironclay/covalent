package covalent.io;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ProxyOutputStream;

/**
 * An output stream that starts buffering to a byte array before switching to a file once its full.
 * 
 * @author Mario Ceste, Jr.
 * @since 1.0
 */
final class BlobOutputStream extends ProxyOutputStream implements Closeable {

    /**
     * The buffer.
     */
    private byte[] buffer;

    /**
     * The buffer's offset.
     */
    private int bufferOffset = 0;

    /**
     * The byte count.
     */
    private long count = 0;

    /**
     * The directory.
     */
    private final Path directory;

    /**
     * The file.
     */
    private Path file;

    /**
     * Sole constructor.
     * 
     * @param bufferSize the buffer size
     * @param directory the directory in which to create temporary files
     */
    protected BlobOutputStream(int bufferSize, Path directory) {
        super(null);
        this.buffer = new byte[bufferSize];
        this.bufferOffset = 0;
        this.directory = directory;
        this.out = new BufferOutputStream();
    }

    /**
     * Write all the bytes from the given {@link InputStream}.
     * 
     * @param in the input stream to read from
     * 
     * @return the number of bytes written
     * 
     * @throws IOException if an I/O occurs in the process of reading from input or writing to this output
     */
    protected long writeFrom(InputStream in) throws IOException {
        return IOUtils.copyLarge(in, this);
    }

    /**
     * Switch to writing to a temporary file.
     *
     * @throws IOException if the file cannot be opened or written to
     */
    private void switchToFile() throws IOException {
        file = Files.createTempFile(directory, "covalent", ".blob");
        out = Files.newOutputStream(file, StandardOpenOption.TRUNCATE_EXISTING);
        out.write(buffer, 0, bufferOffset);
        buffer = null;
    }

    @Override
    protected void beforeWrite(int n) throws IOException {
        if (buffer != null && (bufferOffset + n) > buffer.length) {
            switchToFile();
        }
    }

    @Override
    protected void afterWrite(int n) throws IOException {
        count += n;
    }

    /**
     * Close this output stream and if data was written to a file then request that its deleted when the VM terminates.
     * 
     * @throws IOException if an I/O error occurs
     * 
     * @see java.io.File#deleteOnExit() 
     */
    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            if (file != null) {
                file.toFile().deleteOnExit();
            }
        }
    }

    /**
     * Create a blob using the data that was written to this stream.
     * <p/>
     * This method will flush and close the underlying output stream if its writing to a file.
     * 
     * @return the blob
     */
    protected Blob create() throws IOException {
        if (buffer != null) {
            return new Blob(buffer, null, bufferOffset);
        }

        try {
            out.flush();
        } finally {
            IOUtils.closeQuietly(out);
        }

        return new Blob(null, file, count);
    }

    /**
     * An output stream that writes to its buffer.
     */
    private final class BufferOutputStream extends OutputStream {

        @Override
        public void write(int b) throws IOException {
            buffer[bufferOffset++] = (byte) b;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            System.arraycopy(b, off, buffer, bufferOffset, len);
            bufferOffset += len;
        }

    }

}
