package covalent.io;

import com.google.common.base.Preconditions;
import covalent.io.serialization.Serializer;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;

/**
 * A blob is used to store a large volume of bytes.
 * 
 * @author Mario Ceste, Jr.
 * @since 1.0
 */
public final class Blob {

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
     * The length.
     */
    private final long length;

    /**
     * The file.
     */
    private final Path file;

    /**
     * Sole constructor.
     * 
     * @param buffer the buffer
     * @param file the temporary file
     * @param length the total number of bytes
     */
    Blob(byte[] buffer, Path file, long length) {
        Preconditions.checkArgument(buffer != null ^ file != null);
        Preconditions.checkArgument(length >= 0);
        this.buffer = buffer;
        this.file = file;
        this.length = length;
    }

    /**
     * Return the length of this blob in bytes.
     * 
     * @return the length
     */
    public long length() {
        return length;
    }

    /**
     * Determine whether this blob does not contain any bytes.
     * 
     * @return {@code true} if the {@link #length()} is zero, otherwise {@code false}
     */
    public boolean isEmpty() {
        return length() == 0L;
    }

    /**
     * Open a new {@link InputStream} for reading from this blob.
     * 
     * @return the input stream
     * 
     * @throws IOException if an I/O error occurs in the process of opening the stream
     */
    public InputStream openInputStream() throws IOException {
        if (buffer != null) {
            return new ByteArrayInputStream(buffer, 0, (int) length);
        } else {
            return Files.newInputStream(file);
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
        if (buffer != null) {
            out.write(buffer, 0, (int) length);
        } else {
            try (InputStream in = openInputStream()) {
                IOUtils.copyLarge(in, out);
            }
        }
    }

    /**
     * Create an empty blob.
     * 
     * @return the blob
     */
    public static Blob empty() {
        return new Blob(ArrayUtils.EMPTY_BYTE_ARRAY, null, 0L);
    }

    /**
     * Create a blob by copying all of the bytes from the given {@link InputStream}.
     * 
     * @param in the input stream to read from
     * 
     * @return the blob
     * 
     * @throws IOException if an I/O error occurs in the process of reading from the input or writing to the blob
     */
    public static Blob create(InputStream in) throws IOException {
        try (BlobOutputStream out = new BlobOutputStream(BUFFER_SIZE, TEMPORARY_DIR)) {
            long length = IOUtils.copyLarge(in, out);
            return new Blob(out.buffer, out.file, length);
        }
    }

    /**
     * Create a blob by copying some of the bytes from the given {@link InputStream}.
     * 
     * @param in the input stream to read from
     * @param len the number of bytes to read
     * 
     * @return the blob
     * 
     * @throws IOException if an I/O error occurs in the process of reading from the input or writing to the blob
     * @throws EOFException if the input stream reaches the end before reading all of the bytes
     */
    public static Blob create(InputStream in, long len) throws IOException, EOFException {
        try (BlobOutputStream out = new BlobOutputStream(BUFFER_SIZE, TEMPORARY_DIR)) {
            if (len == IOUtils.copyLarge(in, out, 0, len)) {
                return new Blob(out.buffer, out.file, out.count);
            } else {
                throw new EOFException();
            }
        }
    }

    /**
     * Create a blob that wraps the given bytes.
     * <p/>
     * The new blob will be backed by the given byte array; that is, modifications to the array will cause the blob to
     * be modified and vice versa. The new blob's size will be {@code b.length}.
     * 
     * @param b the blob's contents
     * 
     * @return the blob
     */
    public static Blob wrap(byte[] b) {
        return new Blob(b, null, b.length);
    }

    /**
     * Create a blob that wraps the given file.
     * <p/>
     * The new blob will be backed by the given file; that is, modifications to the file will cause the blob to be 
     * modified and vice versa. The new blob's size will be the {@code Files.size(file)}.
     * 
     * @param file the file
     * 
     * @return the blob
     * 
     * @throws IOException if an I/O error occurs
     * 
     * @see Files#size(java.nio.file.Path) 
     */
    public static Blob wrap(Path file) throws IOException {
        return new Blob(null, file, Files.size(file));
    }

    /**
     * Create a copy of this blob.
     * 
     * @return the blob
     * 
     * @throws IOException if an I/O error occurs
     */
    public Blob copy() throws IOException {
        if (buffer != null) {
            byte[] b = new byte[(int) length];
            System.arraycopy(buffer, 0, b, 0, b.length);
            return new Blob(b, null, b.length);
        } else {
            return create(openInputStream(), length);
        }
    }

    /**
     * Return a {@link Serializer} that converts a blob to and from a stream of bytes.
     * 
     * @return the serializer
     */
    public static Serializer<Blob> serializer() {
        return BlobSerializer.instance;
    }

    /**
     * Serializer implementation for a blob.
     */
    private static class BlobSerializer implements Serializer<Blob> {

        /**
         * The singleton instance.
         */
        private static final BlobSerializer instance = new BlobSerializer();

        @Override
        public Blob read(Input input) throws IOException {
            return Blob.create(input, input.readLong());
        }

        @Override
        public void write(Output output, Blob value) throws IOException {
            output.writeLong(value.length);
            value.copyTo(output);
        }

    }

}
