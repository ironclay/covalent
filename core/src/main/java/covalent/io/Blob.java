package covalent.io;

import com.google.common.base.Preconditions;
import covalent.io.serialization.Serializer;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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
     * Default is 4kb.
     */
    public static final int BUFFER_SIZE = Integer.getInteger("covalent.io.Blob.bufferSize", 0x1000);

    /**
     * Serializer for a {@link Blob}.
     */
    private static final Serializer<Blob> SERIALIZER = new Serializer<Blob>() {

        @Override
        public Blob read(Input input) throws IOException {
            long length = input.readLong();

            if (input.readBoolean()) {
                byte[] array = new byte[(int) length];
                input.readFully(array);
                return Blob.wrap(array);
            } else {
                Blob blob = Blob.empty();

                // copy all of the bytes to the file.
                try (OutputStream out = blob.openOutputStream(false)) {
                    if (IOUtils.copyLarge(input, out, 0, length, input.buffer) != length) {
                        throw new EOFException();
                    }
                }

                return blob;
            }
        }

        @Override
        public void write(Output output, Blob value) throws IOException {
            output.writeLong(value.length);
            output.writeBoolean(value.array != null);

            if (value.file != null) {
                try (InputStream in = value.openInputStream()) {
                    if (IOUtils.copyLarge(in, output, value.array) != value.length) {
                        throw new EOFException();
                    }
                }
            } else {
                output.write(value.array, 0, (int) value.length);
            }
        }

    };

    /**
     * The byte array.
     */
    private byte[] array;

    /**
     * The length.
     */
    private long length;

    /**
     * The file.
     */
    private Path file;

    /**
     * Sole constructor.
     * 
     * @param array the byte array
     * @param file the temporary file
     * @param length the total number of bytes
     */
    private Blob(byte[] array, Path file, long length) {
        Preconditions.checkArgument(array != null ^ file != null);
        Preconditions.checkArgument(length >= 0L);
        this.array = array;
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
     * Open a new {@link InputStream} for reading from this blob.
     * 
     * @return the input stream
     * 
     * @throws IOException if an I/O error occurs in the process of opening the stream
     */
    public InputStream openInputStream() throws IOException {
        return (file != null) ? Files.newInputStream(file) : new ByteArrayInputStream(array, 0, (int) length);
    }

    /**
     * Open a new {@link OutputStream} for writing to this blob.
     * 
     * @return the output stream
     * 
     * @throws IOException if an I/O error occurs in the process of opening the stream
     */
    public OutputStream openOutputStream() throws IOException {
        return openOutputStream(false);
    }

    /**
     * Open a new {@link OutputStream} for writing to this blob.
     * 
     * @param append if {@code true}, then bytes will be written to the end of the blob
     * 
     * @return the output stream
     * 
     * @throws IOException if an I/O error occurs in the process of opening the stream
     */
    public OutputStream openOutputStream(boolean append) throws IOException {
        OutputStream out = null;

        if (file != null) {
            if (append) {
                out = Files.newOutputStream(file, StandardOpenOption.APPEND);
            } else {
                out = Files.newOutputStream(file);
            }
        }

        if (append != true) {
            length = 0L;
        }

        return new BlobOutputStream(out);
    }

    /**
     * Write all of the given bytes to this blob.
     * 
     * @param bytes the bytes to be written
     * 
     * @throws IOException if an I/O error occurs in the process of writing to this blob
     */
    public void write(byte[] bytes) throws IOException {
        try (OutputStream out = openOutputStream(false)) {
            out.write(bytes);
        }
    }

    /**
     * Switch this blob to be backed by a file.
     * 
     * @throws IOException if the file cannot be opened or written to
     */
    public void switchToFile() throws IOException {
        if (file == null) {
            file = Files.createTempFile(TEMPORARY_DIR, "covalent", ".blob");

            if (array != null) {
                try (OutputStream out = Files.newOutputStream(file)) {
                    out.write(array, 0, (int) length);
                } finally {
                    array = null;
                }
            }
        }
    }

    /**
     * Free this blob's array and delete any file that's currently backing it.
     */
    public void free() {
        if (file != null) {
            FileUtils.deleteQuietly(file.toFile());
        } else {
            array = null;
        }
    }

    /**
     * Create a copy of this blob.
     * 
     * @return the blob
     * 
     * @throws IOException if an I/O error occurs
     */
    public Blob copy() throws IOException {
        Blob blob;

        if (file != null) {
            blob = empty();
            blob.switchToFile();
            Files.copy(file, blob.file);
        } else {
            byte[] bytes = new byte[array.length];
            System.arraycopy(array, 0, bytes, 0, (int) length);
            blob = wrap(bytes);
        }

        return blob;
    }

    /**
     * Create a blob that is backed by an empty byte array. Any subsequent modifications will be written to a file.
     * 
     * @return the blob
     */
    public static Blob empty() {
        return wrap(ArrayUtils.EMPTY_BYTE_ARRAY);
    }

    /**
     * Create an empty blob. Any modifications will be written to a byte array before switching to a file.
     * 
     * @return the blob
     */
    public static Blob create() {
        return new Blob(new byte[BUFFER_SIZE], null, 0L);
    }

    /**
     * Create a blob that is backed by the given byte array.
     * <p/>
     * The new blob will be backed by the given byte array; that is, modifications to the array will cause the blob to
     * be modified and vice versa. The new blob's size will be {@code b.length}.
     * 
     * @param array the blob's contents
     * 
     * @return 
     */
    public static Blob wrap(byte[] array) {
        return new Blob(array, null, array.length);
    }

    /**
     * Create a blob that is backed by the given file.
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
     * Return a {@link Serializer} that converts a blob to and from a stream of bytes.
     * 
     * @return the serializer
     */
    public static Serializer<Blob> serializer() {
        return SERIALIZER;
    }

    /**
     * An output stream for writing to this blob. If the underlying output stream is {@code null} then it will try to
     * write to this blob's array if there is space remaining otherwise it will switch to a file. Any bytes that were
     * already written to the array will be transferred to the file.
     */
    private final class BlobOutputStream extends FilterOutputStream {

        /**
         * Sole constructor.
         * 
         * @param out the output stream to delegate to
         */
        private BlobOutputStream(OutputStream out) throws IOException {
            super(out);
        }

        @Override
        public void write(int b) throws IOException {
            beforeWrite(1);

            if (out != null) {
                out.write(b);
            } else {
                array[(int) length] = (byte) b;
            }

            afterWrite(1);
        }

        @Override
        public void write(byte[] b) throws IOException {
            write(b, 0, b.length);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            beforeWrite(len);

            if (out != null) {
                out.write(b, off, len);
            } else {
                System.arraycopy(b, off, array, (int) length, len);
            }

            afterWrite(len);
        }

        @Override
        public void flush() throws IOException {
            if (out != null) {
                out.flush();
            }
        }

        @Override
        public void close() throws IOException {
            if (out != null) {
                out.close();
            }
        }

        /**
         * Start writing to a file if the data to be written can't fit into the remainder of this blob's array.
         * 
         * @param n the number of bytes to be written
         * 
         * @throws IOException if an I/O error occurs
         */
        protected void beforeWrite(int n) throws IOException {
            if (out == null && (length + n) > array.length) {
                out = Files.newOutputStream(file);

                if (length != 0L) {
                    // transfer bytes to the file.
                    out.write(array, 0, (int) length);
                    out.flush();
                }
            }
        }

        /**
         * Increment the total number of bytes that have been written to this blob.
         * 
         * @param n the number of bytes that were written
         * 
         * @throws IOException if an I/O error occurs
         */
        protected void afterWrite(int n) throws IOException {
            length += n;
        }

    }

}