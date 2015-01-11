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
import java.nio.file.Path;
import org.apache.commons.io.IOUtils;

/**
 * A clob is used to store a large volume of characters. It keeps the data in memory until a threshold is exceeded.
 * 
 * @author Mario Ceste, Jr.
 * @since 1.0
 */
public final class Clob implements Closeable {

    /**
     * The blob.
     */
    private final Blob blob;

    /**
     * The encoding.
     */
    private final Charset charset;

    /**
     * The length.
     */
    private long length;

    /**
     * Sole constructor.
     * 
     * @param blob the blob
     * @param charset the encoding
     * @param length the initial size, typically zero
     */
    private Clob(Blob blob, Charset charset, long length) {
        this.blob = blob;
        this.charset = charset;
        this.length = length;
    }

    /**
     * Return the length of this clob.
     * 
     * @return the clob's length
     */
    public long length() {
        return length;
    }

    /**
     * Determine whether this clob does not contain any character.
     * 
     * @return {@code true} if the length is zero, or {@code false} otherwise
     */
    public boolean isEmpty() {
        return length() == 0L;
    }

    /**
     * Return the {@link Charset charset} that is used by this clob to convert between characters and bytes.
     * 
     * @return the charset
     */
    public Charset charset() {
        return charset;
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
        return new InputStreamReader(blob.openInputStream(), charset);
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
        return new OutputStreamWriter(blob.openOutputStream(), charset);
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
     * Write the desired number of characters from the given {@link Reader} to this clob.
     * 
     * @param in the character stream to read from
     * @param len the number of characters to read
     * 
     * @throws IOException if an I/O occurs in the process of reading from the input or writing to this clob
     */
    public void writeFrom(Reader in, long len) throws IOException {
        try (Writer out = openWriter()) {
            IOUtils.copyLarge(in, out, 0L, len);
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

    @Override
    public void close() throws IOException {
        blob.close();
    }

    /**
     * Create an empty clob.
     * 
     * @param charset the character encoding
     * 
     * @return the clob
     */
    public static Clob create(Charset charset) {
        return new Clob(Blob.create(), charset, 0L);
    }

}
