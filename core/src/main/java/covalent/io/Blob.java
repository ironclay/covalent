package covalent.io;

import com.google.common.base.Preconditions;
import covalent.io.serialization.Serializer;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.input.ClosedInputStream;
import org.apache.commons.io.output.ClosedOutputStream;
import org.apache.commons.io.output.ProxyOutputStream;
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
    public static final File TEMPORARY_DIR = FileUtils.getTempDirectory();

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
            Blob blob = create();

            try (OutputStream out = blob.openOutputStream(false)) {
                long length = input.readLong();

                // read all of the bytes.
                for (int len = 0; length > 0; length -= len) {
                    len = (int) Math.min(input.buffer.length, length - len);
                    input.readFully(input.buffer, 0, len);
                    out.write(input.buffer, 0, len);
                }
            }

            return blob;
        }

        @Override
        public void write(Output output, Blob value) throws IOException {
            output.writeLong(value.length);

            try (InputStream in = value.openInputStream()) {
                for (long n = 0; n < value.length; n += output.buffer.length) {
                    int len = (int) Math.min(value.length - n, output.buffer.length);
                    in.read(output.buffer, 0, len);
                    output.write(output.buffer, 0, len);
                }
            }
        }

    };

    /**
     * The buffer.
     */
    private ByteBuffer buffer;

    /**
     * The length.
     */
    private long length;

    /**
     * The file.
     */
    private File file;

    /**
     * Sole constructor.
     * 
     * @param bb the byte buffer
     * @param file the temporary file
     * @param length the total number of bytes
     */
    private Blob(ByteBuffer bb, File file, long length) {
        Preconditions.checkArgument(bb != null ^ file != null);
        Preconditions.checkArgument(bb == null || bb.hasArray());
        Preconditions.checkArgument(length >= 0L);
        this.buffer = bb;
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
        if (file != null) {
            return new FileInputStream(file);
        } else if (buffer != null) {
            return new ByteArrayInputStream(buffer.array(), buffer.arrayOffset(), (int) length);
        } else {
            return ClosedInputStream.CLOSED_INPUT_STREAM;
        }
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
            out = new FileOutputStream(file, append);
        } else if (buffer != null) {
            out = new BufferOutputStream();
            buffer.position(append ? (int) length : 0);
        } else {
            throw new IOException("blob is closed");
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
            file = File.createTempFile("covalent", ".blob", TEMPORARY_DIR);

            if (buffer != null) {
                // transfer bytes to the file.
                try (OutputStream out = new FileOutputStream(file)) {
                    out.write(buffer.array(), buffer.arrayOffset(), (int) length);
                } finally {
                    buffer = null;
                }
            }
        }
    }

    /**
     * Free this blob's buffer and delete any file that's currently backing it.
     */
    public void free() {
        if (file != null) {
            FileUtils.deleteQuietly(file);
        } else {
            buffer = null;
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
        if (file != null) {
            File f = createTempFile();
            FileUtils.copyFile(file, f);
            return new Blob(null, f, length);
        } else if (buffer != null) {
            ByteBuffer bb = ByteBuffer.allocate(buffer.capacity());
            bb.put(buffer.array(), buffer.arrayOffset(), (int) length);
            return new Blob(bb, null, length);
        }

        return Blob.empty();
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
        return new Blob(ByteBuffer.allocate(BUFFER_SIZE), null, 0L);
    }

    /**
     * Create a blob that is backed by the given byte array.
     * <p/>
     * The new blob will be backed by the given byte array; that is, modifications to the array will cause the blob to
     * be modified and vice versa. The new blob's size will be {@code b.length}.
     * 
     * @param array the blob's contents
     * 
     * @return the blob
     */
    public static Blob wrap(byte[] array) {
        return new Blob(ByteBuffer.wrap(array), null, array.length);
    }

    /**
     * Create a blob that is backed by the given file.
     * <p/>
     * The new blob will be backed by the given file; that is, modifications to the file will cause the blob to be
     * modified and vice versa. The new blob's size will be {@code Files.size(file)}.
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
        return new Blob(null, file.toFile(), Files.size(file));
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
     * Create a temporary file that will hold the contents of a blob.
     * 
     * @return the temporary file
     * 
     * @throws IOException if an I/O error occurs
     */
    private static File createTempFile() throws IOException {
        FileUtils.forceMkdir(TEMPORARY_DIR);
        return File.createTempFile("covalent", ".blob", TEMPORARY_DIR);
    }

    /**
     * An output stream for writing data to this blob.
     * <p/>
     * Before writing any bytes to the buffer this class verifies that there is sufficient space remaining. If there
     * isn't then it will switch to writing to a file and any bytes that were previously written will be transferred.
     */
    private final class BlobOutputStream extends ProxyOutputStream {

        /**
         * Sole constructor.
         * 
         * @param out the output stream to delegate to
         */
        private BlobOutputStream(OutputStream out) throws IOException {
            super(out);
        }

        @Override
        protected void beforeWrite(int n) throws IOException {
            if (file == null && n > buffer.remaining()) {
                file = createTempFile();
                out = new FileOutputStream(file);

                if (length != 0L) {
                    // transfer bytes to the file.
                    out.write(buffer.array(), buffer.arrayOffset(), (int) length);
                    out.flush();
                }
            }
        }

        @Override
        protected void afterWrite(int n) throws IOException {
            length += n;
        }

    }

    /**
     * An output stream in which the data is written into the blob's buffer.
     */
    private final class BufferOutputStream extends OutputStream {

        @Override
        public void write(int b) throws IOException {
            buffer.put((byte) b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            buffer.put(b, off, len);
        }

    }

}