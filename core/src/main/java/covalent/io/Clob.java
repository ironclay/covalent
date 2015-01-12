package covalent.io;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.apache.commons.io.IOUtils;

/**
 * A clob is used to store a large volume of characters.
 * 
 * @author Mario Ceste, Jr.
 * @since 1.0
 */
public final class Clob implements Closeable {

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
     * Sole constructor.
     * 
     * @param blob the blob
     */
    private Clob(Blob blob) {
        this.blob = blob;
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
     * Open a new {@link Writer} for writing to this clob.
     * 
     * @return the writer
     * 
     * @throws IOException if an I/O error occurs in the process of opening the writer
     * 
     * @see OutputStreamWriter#OutputStreamWriter(java.io.OutputStream, java.nio.charset.Charset) 
     */
    public Writer openWriter() throws IOException {
        return new OutputStreamWriter(blob.openOutputStream(), DEFAULT_CHARSET);
    }

    /**
     * Write all of the characters from the given {@link CharSequence} to this clob.
     * 
     * @param csq the character sequence to read from
     * 
     * @throws IOException if an I/O occurs in the process of writing to this clob
     */
    public void write(CharSequence csq) throws IOException {
        try (Writer out = openWriter()) {
            IOUtils.write(csq, out);
        }
    }

    /**
     * Write all of the characters from the given {@link Reader} to this clob.
     * 
     * @param in the character stream to read from
     * 
     * @throws IOException if an I/O occurs in the process of reading from the input or writing to this clob
     */
    public void writeFrom(Reader in) throws IOException {
        try (Writer out = openWriter()) {
            IOUtils.copyLarge(in, out);
        }
    }

    /**
     * Copy the text from this clob to another clob.
     * 
     * @param clob the clob to write to
     * 
     * @throws IOException if an I/O error occurs in the process of reading or writing text
     */
    public void copyTo(Clob clob) throws IOException {
        try (Writer out = clob.openWriter()) {
            copyTo(out);
        }
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
     * Copy the text from this clob to the given {@link File}.
     * 
     * @param file the file to write to
     * 
     * @throws IOException if an I/O occurs in the process of reading from this clob or writing to the file
     */
    public void copyTo(File file) throws IOException {
        blob.copyTo(file);
    }

    /**
     * Copy the text from this clob to the given {@link Path}.
     * 
     * @param file the file to write to
     * 
     * @throws IOException if an I/O occurs in the process of reading from this clob or writing to the file
     */
    public void copyTo(Path file) throws IOException {
        blob.copyTo(file);
    }

    /**
     * Read the contents of this clob from the given input stream.
     * 
     * @param input the Input to read from
     * 
     * @throws IOException if an I/O occurs in the process of deserializing this blob
     * 
     * @see #writeTo(covalent.io.Output) 
     */
    public void readFrom(Input input) throws IOException {
        blob.readFrom(input);
    }

    /**
     * Write the contents of this clob to the given output stream.
     * 
     * @param output the Output to write to
     * 
     * @throws IOException if an I/O occurs in the process of serializing this blob
     * 
     * @see Blob#writeTo(covalent.io.Output) 
     */
    public void writeTo(Output output) throws IOException {
        blob.writeTo(output);
    }

    /**
     * Close this clob and release any resources associated with it.
     * 
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        blob.close();
    }

    /**
     * Create an empty clob.
     * 
     * @return the clob
     */
    public static Clob create() {
        return new Clob(Blob.create());
    }

    /**
     * Create a clob that wraps the given string.
     * 
     * @param s the String to wrap
     * 
     * @return the clob
     */
    public static Clob create(String s) {
        return new Clob(Blob.create(s.getBytes(DEFAULT_CHARSET)));
    }

}
