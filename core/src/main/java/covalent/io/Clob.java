package covalent.io;

import com.google.common.base.Preconditions;
import covalent.io.serialization.Serializer;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.CharSequenceReader;

/**
 * A clob is used to store a large volume of characters. It's backed by a blob.
 * 
 * @author Mario Ceste, Jr.
 * @since 1.0
 */
public final class Clob {

    /**
     * Indicates the character encoding that is used to convert between characters and bytes.
     * <p/>
     * Default is UTF-8.
     */
    public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    /**
     * The blob.
     */
    private final Blob blob;

    /**
     * The length.
     */
    private final long length;

    /**
     * Sole constructor.
     * 
     * @param blob the blob
     * @param length the number of characters
     */
    private Clob(Blob blob, long length) {
        Preconditions.checkArgument(blob != null);
        Preconditions.checkArgument(length >= 0);
        this.blob = blob;
        this.length = length;
    }

    /**
     * Return the length of this clob in characters.
     * 
     * @return the length
     */
    public long length() {
        return length;
    }

    /**
     * Open a new {@link Reader} for reading from this clob.
     * 
     * @return the reader
     * 
     * @throws IOException if an I/O error occurs in the process of opening the reader
     * 
     * @see InputStreamReader#InputStreamReader(java.io.InputStream, java.nio.charset.Charset) 
     */
    public Reader openReader() throws IOException {
        return new InputStreamReader(blob.openInputStream(), DEFAULT_CHARSET);
    }

    /**
     * Copy the text from this clob to the given {@link Writer}.
     * 
     * @param out the character stream to write to
     * 
     * @throws IOException if an I/O occurs in the process of reading from this clob or writing to the output
     */
    public void copyTo(Writer out) throws IOException {
        try (Reader in = openReader()) {
            IOUtils.copyLarge(in, out);
        }
    }

    /**
     * Create an empty clob.
     * 
     * @return the clob
     */
    public static Clob empty() {
        return new Clob(Blob.empty(), 0L);
    }

    /**
     * Create a clob by copying the given character sequence.
     * 
     * @param csq the character sequence
     * 
     * @return the clob
     * 
     * @throws IOException if an I/O error occurs in the process of writing to the clob
     */
    public static Clob create(CharSequence csq) throws IOException {
        return create(new CharSequenceReader(csq));
    }

    /**
     * Create a clob by copying all of the characters from the given {@link Reader}.
     * 
     * @param in the character stream to read from
     * 
     * @return the clob
     * 
     * @throws IOException if an I/O error occurs in the process of reading from the input or writing to the clob
     */
    public static Clob create(Reader in) throws IOException {
        try (BlobOutputStream out = new BlobOutputStream(Blob.BUFFER_SIZE, Blob.TEMPORARY_DIR)) {
            long length;

            // copy all the characters as bytes.
            try (Writer output = new OutputStreamWriter(out, DEFAULT_CHARSET)) {
                length = IOUtils.copyLarge(in, output);
            }

            return new Clob(new Blob(out.buffer, out.file, out.count), length);
        }
    }

    /**
     * Create a copy of this clob.
     * 
     * @return the clob
     * 
     * @throws IOException if an I/O error occurs
     */
    public Clob copy() throws IOException {
        return new Clob(blob.copy(), length);
    }

    /**
     * Return a {@link Serializer} that converts a clob to and from a stream of bytes.
     * 
     * @return the serializer
     */
    public static Serializer<Clob> serializer() {
        return ClobSerializer.instance;
    }

    /**
     * Serializer implementation for a clob.
     */
    private static class ClobSerializer implements Serializer<Clob> {

        /**
         * The singleton instance.
         */
        private static final ClobSerializer instance = new ClobSerializer();

        @Override
        public Clob read(Input input) throws IOException {
            long length = input.readLong();

            if (length != 0) {
                return new Clob(Blob.serializer().read(input), length);
            } else {
                return empty();
            }
        }

        @Override
        public void write(Output output, Clob value) throws IOException {
            output.writeLong(value.length);

            if (value.length != 0) {
                Blob.serializer().write(output, value.blob);
            }
        }

    }

}
