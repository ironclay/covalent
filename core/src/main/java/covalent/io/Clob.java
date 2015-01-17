package covalent.io;

import covalent.io.serialization.Serializer;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ProxyWriter;

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
     * Serializer for a {@link Clob}.
     */
    private static final Serializer<Clob> SERIALIZER = new Serializer<Clob>() {

        @Override
        public Clob read(Input input) throws IOException {
            long length = input.readLong();
            return new Clob(Blob.serializer().read(input), length);
        }

        @Override
        public void write(Output output, Clob value) throws IOException {
            output.writeLong(value.length);
            Blob.serializer().write(output, value.blob);
        }

    };

    /**
     * The blob.
     */
    private final Blob blob;

    /**
     * The length.
     */
    private long length;

    /**
     * Sole constructor.
     * 
     * @param blob the blob
     * @param length the number of characters
     */
    private Clob(Blob blob, long length) {
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
     */
    public Reader openReader() throws IOException {
        return new InputStreamReader(blob.openInputStream(), DEFAULT_CHARSET);
    }

    /**
     * Open a new {@link Writer} for writing to this clob.
     * 
     * @param append if {@code true}, then characters will be written to the end of the clob
     * 
     * @return the writer
     * 
     * @throws IOException if an I/O error occurs in the process of opening the writer
     */
    public Writer openWriter(final boolean append) throws IOException {
        Writer out = new OutputStreamWriter(blob.openOutputStream(append), DEFAULT_CHARSET);

        if (append != true) {
            length = 0L;
        }

        return new ProxyWriter(out) {

            @Override
            protected void afterWrite(int n) throws IOException {
                length += n;
            }

        };
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
     * Create an empty clob.
     * 
     * @return the clob
     */
    public Clob create() {
        return new Clob(Blob.create(), 0L);
    }

    /**
     * Return a {@link Serializer} that converts a clob to and from a stream of bytes.
     * 
     * @return the serializer
     */
    public static Serializer<Clob> serializer() {
        return SERIALIZER;
    }

}
