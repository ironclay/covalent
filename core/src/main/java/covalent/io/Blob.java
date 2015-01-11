package covalent.io;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.io.output.ProxyOutputStream;

/**
 * A blob is used to store a large volume of bytes. It keeps the data in memory until a threshold is exceeded.
 * <p/>
 * This class is NOT thread-safe.
 * 
 * @author Mario Ceste, Jr.
 * @since 1.0
 */
public final class Blob implements Closeable {

    /**
     * Indicates the path to the directory in which to create temporary files.
     * <p/>
     * Default is Java's temporary-file directory.
     */
    public static final Path TEMPORARY_DIR = FileUtils.getTempDirectory().toPath();

    /**
     * Indicates the size of the buffer to use.
     * <p/>
     * Default is 64kb.
     */
    public static final int BUFFER_SIZE = Integer.getInteger("covalent.io.Blob.bufferSize", 0x10000);

    /**
     * The buffer.
     */
    private final byte[] buffer;

    /**
     * The size.
     */
    private long size;

    /**
     * The temporary file.
     */
    private Path file;

    /**
     * Sole constructor.
     * 
     * @param buffer the buffer
     * @param size the initial size, typically zero
     */
    private Blob(byte[] buffer, long size) {
        this.buffer = buffer;
        this.size = size;
    }

    /**
     * Return the size of this blob in bytes.
     * 
     * @return the blob's size
     */
    public long size() {
        return size;
    }

    /**
     * Determine whether this blob does not contain any bytes.
     * 
     * @return {@code true} if the size is zero, or {@code false} otherwise
     */
    public boolean isEmpty() {
        return size() == 0L;
    }

    /**
     * Open a new {@link InputStream} for reading from this blob.
     * 
     * @return the input stream
     * 
     * @throws IOException if an I/O error occurs in the process of opening the stream
     * 
     * @see Files#newInputStream(java.nio.file.Path, java.nio.file.OpenOption[]) 
     * @see ByteArrayInputStream
     */
    public InputStream openInputStream() throws IOException {
        return (file != null) ? Files.newInputStream(file) : new ByteArrayInputStream(buffer, 0, (int) size);
    }

    /**
     * Open a new {@link OutputStream} for writing to this blob.
     * 
     * @return the output stream
     * 
     * @throws IOException if an I/O error occurs in the process of opening the stream
     */
    public OutputStream openOutputStream() throws IOException {
        return new BlobOutputStream();
    }

    /**
     * Write all of the given bytes to this blob.
     * 
     * @param bytes the bytes to write
     * 
     * @throws IOException if an I/O occurs in the process of writing to this blob
     */
    public void write(byte[] bytes) throws IOException {
        try (OutputStream out = openOutputStream()) {
            out.write(bytes);
        }
    }

    /**
     * Write all of the bytes from the given {@link InputStream} to this blob.
     * 
     * @param in the input stream to read from
     * 
     * @throws IOException if an I/O occurs in the process of reading from the input or writing to this blob
     */
    public void writeFrom(InputStream in) throws IOException {
        try (OutputStream out = openOutputStream()) {
            IOUtils.copyLarge(in, out);
        }
    }

    /**
     * Write the desired number of bytes from the given {@link InputStream} to this blob.
     * 
     * @param in the input stream to read from
     * @param len the number of bytes to read
     * 
     * @throws IOException if an I/O occurs in the process of reading from the input or writing to this blob
     */
    public void writeFrom(InputStream in, long len) throws IOException {
        if (len > buffer.length) {
            try (OutputStream out = openOutputStream()) {
                IOUtils.copyLarge(in, out, 0L, len);
            }
        } else {
            IOUtils.readFully(in, buffer, 0, (int) len);
            this.size = len;
        }
    }

    /**
     * Copy the contents of this blob to the given {@link File file}.
     * 
     * @param file the file to write to
     * 
     * @throws IOException if an I/O error occurs in the process of reading from this blob or writing to the file
     */
    public void copyTo(File file) throws IOException {
        try (FileOutputStream out = new FileOutputStream(file)) {
            copyTo(out);
        }
    }

    /**
     * Copy the contents of this blob to the given {@link Path file}.
     * 
     * @param file the file to write to
     * 
     * @throws IOException if an I/O error occurs in the process of reading from this blob or writing to the file
     */
    public void copyTo(Path file) throws IOException {
        try (OutputStream out = Files.newOutputStream(file)) {
            copyTo(out);
        }
    }

    /**
     * Copy the contents of this blob to the given {@link OutputStream}.
     * 
     * @param out the output stream to write to
     * 
     * @throws IOException if an I/O error occurs in the process of reading from this blob or writing to the output
     */
    public void copyTo(OutputStream out) throws IOException {
        if (size > buffer.length) {
            try (InputStream in = openInputStream()) {
                IOUtils.copyLarge(in, out);
            }
        } else {
            out.write(buffer, 0, (int) size);
        }
    }

    /**
     * Copy the contents of this blob to another blob.
     * 
     * @param blob the blob to write to
     * 
     * @throws IOException if an I/O error occurs in the process of reading or writing bytes
     */
    public void copyTo(Blob blob) throws IOException {
        try (OutputStream out = blob.openOutputStream()) {
            copyTo(out);
        }
    }

    @Override
    public void close() throws IOException {
        if (file != null) {
            if (Files.deleteIfExists(file)) {
                file = null;
            }
        }
    }

    /**
     * Create an empty blob.
     * 
     * @return the blob
     */
    public static Blob create() {
        byte[] buffer = new byte[BUFFER_SIZE];
        return new Blob(buffer, 0L);
    }

    /**
     * Create a blob that wraps the given bytes.
     * <p/>
     * The new blob will be backed by the given byte array; that is, modifications to the blob will cause the array to
     * be modified and vice versa. The new blob's size will be {@code b.length}.
     * 
     * @param b the blob's contents
     * 
     * @return the blob
     */
    public static Blob create(byte[] b) {
        return new Blob(b, b.length);
    }

    /**
     * An output stream for writing bytes to this blob.
     */
    private final class BlobOutputStream extends ProxyOutputStream {

        /**
         * The buffer.
         */
        private BufferOutputStream buffer;

        /**
         * Default constructor.
         */
        private BlobOutputStream() {
            super(null);
            out = buffer = new BufferOutputStream();
        }

        /**
         * Switch to writing to the temporary file.
         * 
         * @throws IOException if the file cannot be opened or written to
         */
        private void switchToFile() throws IOException {
            out = Files.newOutputStream(file);
            buffer.writeTo(out);
            buffer = null;
        }

        @Override
        protected void beforeWrite(int n) throws IOException {
            if (buffer != null && buffer.canWrite(n) != true) {
                switchToFile();
            }
        }

        @Override
        protected void afterWrite(int n) throws IOException {
            size += n;
        }

    }

    /**
     * An output stream that writes to its buffer.
     */
    private final class BufferOutputStream extends OutputStream {

        /**
         * The offset.
         */
        private int offset = 0;

        @Override
        public void write(int b) throws IOException {
            buffer[offset++] = (byte) b;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            System.arraycopy(b, off, buffer, offset, len);
            offset += len;
        }

        /**
         * Write the entire contents of this byte stream to the given output stream.
         * 
         * @param out the output stream to write to
         * 
         * @throws IOException if an I/O error occurs in the process of writing to the output
         */
        public void writeTo(OutputStream out) throws IOException {
            out.write(buffer, 0, offset);
        }

        /**
         * Determine whether the buffer can hold the given number of additional bytes.
         * 
         * @param n the number of bytes to be written
         * 
         * @return {@code true} if the buffer can hold the bytes
         */
        public boolean canWrite(int n) {
            return (offset + n) < buffer.length;
        }

    }

}
