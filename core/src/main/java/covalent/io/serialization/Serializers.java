package covalent.io.serialization;

import covalent.io.Blob;
import covalent.io.Clob;
import covalent.io.Input;
import covalent.io.Output;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Date;

/**
 * Factory for out-of-the-box {@link Serializer} implementations.
 * 
 * @author Mario Ceste, Jr.
 * @since 1.0
 */
public final class Serializers {

    /**
     * Serializer for a Java {@code int}.
     */
    public static final Serializer<Integer> INTEGER = new Serializer<Integer>() {

        @Override
        public Integer read(Input input) throws IOException {
            return input.readInt();
        }

        @Override
        public void write(Output output, Integer value) throws IOException {
            output.writeInt(value);
        }

    };

    /**
     * Serializer for a Java {@code short}.
     */
    public static final Serializer<Short> SHORT = new Serializer<Short>() {

        @Override
        public Short read(Input input) throws IOException {
            return input.readShort();
        }

        @Override
        public void write(Output output, Short value) throws IOException {
            output.writeShort(value);
        }

    };

    /**
     * Serializer for a Java {@code long}.
     */
    public static final Serializer<Long> LONG = new Serializer<Long>() {

        @Override
        public Long read(Input input) throws IOException {
            return input.readLong();
        }

        @Override
        public void write(Output output, Long value) throws IOException {
            output.writeLong(value);
        }

    };

    /**
     * Serializer for a Java {@code float}.
     */
    public static final Serializer<Float> FLOAT = new Serializer<Float>() {

        @Override
        public Float read(Input input) throws IOException {
            return input.readFloat();
        }

        @Override
        public void write(Output output, Float value) throws IOException {
            output.writeFloat(value);
        }

    };

    /**
     * Serializer for a Java {@code double}.
     */
    public static final Serializer<Double> DOUBLE = new Serializer<Double>() {

        @Override
        public Double read(Input input) throws IOException {
            return input.readDouble();
        }

        @Override
        public void write(Output output, Double value) throws IOException {
            output.writeDouble(value);
        }

    };

    /**
     * Serializer for a Java {@code boolean}.
     */
    public static final Serializer<Boolean> BOOLEAN = new Serializer<Boolean>() {

        @Override
        public Boolean read(Input input) throws IOException {
            return input.readBoolean();
        }

        @Override
        public void write(Output output, Boolean value) throws IOException {
            output.writeBoolean(value);
        }

    };

    /**
     * Serializer for a Java {@code byte}.
     */
    public static final Serializer<Byte> BYTE = new Serializer<Byte>() {

        @Override
        public Byte read(Input input) throws IOException {
            return input.readByte();
        }

        @Override
        public void write(Output output, Byte value) throws IOException {
            output.writeByte(value);
        }

    };

    /**
     * Serializer for a sequence of Unicode characters encoded using a modified UTF-8 format.
     * 
     * @see java.io.DataInput#readUTF() 
     * @see java.io.DataOutput#writeUTF(java.lang.String) 
     */
    public static final Serializer<String> UTF8 = new Serializer<String>() {

        @Override
        public String read(Input input) throws IOException {
            return input.readUTF();
        }

        @Override
        public void write(Output output, String value) throws IOException {
            output.writeUTF(value);
        }

    };

    /**
     * Serializer for a sequence of ASCII characters.
     */
    public static final Serializer<String> ASCII = new Serializer<String>() {

        @Override
        public String read(Input input) throws IOException {
            char[] cbuf = new char[input.readInt()];

            for (int i = 0; i < cbuf.length; i++) {
                cbuf[i] = (char) input.readByte();
            }

            return new String(cbuf);
        }

        @Override
        public void write(Output output, String value) throws IOException {
            output.writeInt(value.length());

            for (int i = 0, len = value.length(); i < len; i++) {
                char c = value.charAt(i);
                output.writeByte(c);
            }
        }

    };

    /**
     * Serializer for a {@link Date}.
     */
    public static final Serializer<Date> DATE = new Serializer<Date>() {

        @Override
        public Date read(Input input) throws IOException {
            long millis = input.readLong();
            return new Date(millis);
        }

        @Override
        public void write(Output output, Date value) throws IOException {
            output.writeLong(value.getTime());
        }

    };

    /**
     * Serializer for a {@link URL}.
     */
    public static final Serializer<URL> URL = new Serializer<URL>() {

        @Override
        public URL read(Input input) throws IOException {
            String spec = input.readUTF();
            return new URL(spec);
        }

        @Override
        public void write(Output output, URL value) throws IOException {
            String spec = value.toString();
            output.writeUTF(spec);
        }

    };

    /**
     * Serializer for a {@link Blob}.
     */
    public static final Serializer<Blob> BLOB = new Serializer<Blob>() {

        @Override
        public Blob read(Input input) throws IOException {
            Blob blob = Blob.create();
            blob.readFrom(input);
            return blob;
        }

        @Override
        public void write(Output output, Blob value) throws IOException {
            value.writeTo(output);
        }

    };

    /**
     * Serializer for a {@link Clob}.
     */
    public static final Serializer<Clob> CLOB = new Serializer<Clob>() {

        @Override
        public Clob read(Input input) throws IOException {
            Clob clob = Clob.create();
            clob.readFrom(input);
            return clob;
        }

        @Override
        public void write(Output output, Clob value) throws IOException {
            value.writeTo(output);
        }

    };

}
