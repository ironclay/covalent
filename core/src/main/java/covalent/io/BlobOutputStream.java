package covalent.io;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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
    protected byte[] buffer;

    /**
     * The byte count.
     */
    protected long count = 0;

    /**
     * The directory.
     */
    private final Path directory;

    /**
     * The file.
     */
    protected Path file;

    /**
     * Sole constructor.
     * 
     * @param bufferSize the buffer size
     * @param directory the directory in which to create temporary files
     */
    protected BlobOutputStream(int bufferSize, Path directory) {
        super(null);
        this.buffer = new byte[bufferSize];
        this.directory = directory;
        this.out = new ByteArrayOutputStream();
    }

    /**
     * Switch to writing to a temporary file.
     *
     * @throws IOException if the file cannot be opened or written to
     */
    private void switchToFile() throws IOException {
        file = Files.createTempFile(directory, "covalent", ".blob");
        out = Files.newOutputStream(file, StandardOpenOption.TRUNCATE_EXISTING);
        out.write(buffer, 0, (int) count);
        buffer = null;
    }

    @Override
    protected void beforeWrite(int n) throws IOException {
        if (buffer != null && (count + n) > buffer.length) {
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
     * An output stream that writes to the byte array.
     */
    private final class ByteArrayOutputStream extends OutputStream {

        @Override
        public void write(int b) throws IOException {
            buffer[(int) count++] = (byte) b;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            System.arraycopy(b, off, buffer, (int) count, len);
        }

    }

}
