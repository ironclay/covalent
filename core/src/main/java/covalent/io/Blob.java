package covalent.io;

import covalent.io.serialization.Serializer;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

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
            Blob blob = Blob.create();
            long length = input.readLong();

            if (length > blob.array.length) {
                blob.switchToFile();

                // copy all of the bytes to the file.
                try (OutputStream out = blob.openOutputStream(false)) {
                    if (IOUtils.copyLarge(input, out, 0, length, blob.array) != length) {
                        throw new EOFException();
                    }
                }
            } else {
                // read the bytes into its array.
                input.readFully(blob.array, 0, (int) (blob.length = length));
            }

            return blob;
        }

        @Override
        public void write(Output output, Blob value) throws IOException {
            output.writeLong(value.length);

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
     */
    private Blob() {
        this.array = new byte[BUFFER_SIZE];
        this.length = 0;
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
        if (file != null) {
            try (InputStream in = openInputStream()) {
                if (IOUtils.copyLarge(in, out, array) != length) {
                    throw new EOFException();
                }
            }
        } else {
            out.write(array, 0, (int) length);
        }
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
     * Write all of the bytes from the given {@link InputStream} to this blob.
     * 
     * @param in the input stream to read from
     * 
     * @return the total number of bytes read from the input stream
     * 
     * @throws IOException if an I/O error occurs in the process of reading from the input or writing to this blob
     */
    public long writeFrom(InputStream in) throws IOException {
        try (OutputStream out = openOutputStream(false)) {
            return IOUtils.copyLarge(in, out);
        }
    }

    /**
     * Create an empty blob.
     * 
     * @return the blob
     */
    public static Blob create() {
        return new Blob();
    }

    /**
     * Create a copy of this blob.
     * 
     * @return the blob
     * 
     * @throws IOException if an I/O error occurs
     */
    public Blob copy() throws IOException {
        Blob blob = create();

        if (file != null) {
            try (InputStream in = openInputStream()) {
                blob.writeFrom(in);
            }
        } else {
            System.arraycopy(array, 0, blob.array, 0, (int) (blob.length = length));
        }

        return blob;
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
     * Switch this blob to be backed by a file.
     * 
     * @throws IOException if the file cannot be opened or written to
     */
    public void switchToFile() throws IOException {
        if (file == null) {
            file = Files.createTempFile(TEMPORARY_DIR, "covalent", ".blob");

            if (length != 0) {
                try (OutputStream out = Files.newOutputStream(file)) {
                    out.write(array, 0, (int) length);
                }
            }
        }
    }

    /**
     * An output stream for writing to this blob. If the underlying output stream is {@code null} then it will try to
     * write to this blob's array if there is space remaining otherwise it will switch to a file. Any bytes that were
     * already written to the array will be copied to the file.
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
        public void close() throws IOException {
            if (out != null) {
                out.close();
            }
        }

        @Override
        public void write(int b) throws IOException {
            beforeWrite(1);

            if (out != null) {
                super.write(b);
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
                super.write(b, off, len);
            } else {
                System.arraycopy(b, off, array, (int) length, len);
            }

            afterWrite(len);
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
                out = Files.newOutputStream(file, StandardOpenOption.TRUNCATE_EXISTING);

                if (length != 0L) {
                    out.write(array, 0, (int) length);
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