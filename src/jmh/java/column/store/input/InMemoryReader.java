package column.store.input;

import column.store.api.column.*;
import column.store.api.query.Query;
import column.store.api.read.*;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Reads all records into memory column by column.
 */
public class InMemoryReader implements Reader {

    private final Map<Column, ByteReader> readers;
    private Path root;

    private boolean consumed = true;
    private boolean hasNext;

    public InMemoryReader() {
        this.readers = new HashMap<>();
    }

    @Override
    public void query(final Query query) {
        if (!query.filters().isEmpty()) {
            throw new UnsupportedOperationException("For performance reasons, this reader does not support filters");
        }
        readers.clear();
        root = query.filePath();
    }

    @Override
    public BooleanByteReader of(final BooleanColumn column) {
        return (BooleanByteReader) readers.computeIfAbsent(column, col -> {
            var bytes = readAllBytesOf(column);
            return new BooleanByteReader(bytes);
        });
    }

    @Override
    public DoubleByteReader of(final DoubleColumn column) {
        return (DoubleByteReader) readers.computeIfAbsent(column, col -> {
            var bytes = readAllBytesOf(column);
            return new DoubleByteReader(bytes);
        });
    }

    @Override
    public IdByteReader of(final IdColumn column) {
        return (IdByteReader) readers.computeIfAbsent(column, col -> {
            var bytes = readAllBytesOf(column);
            return new IdByteReader(bytes);
        });
    }

    @Override
    public LongByteReader of(final LongColumn column) {
        return (LongByteReader) readers.computeIfAbsent(column, col -> {
            var bytes = readAllBytesOf(column);
            return new LongByteReader(bytes);
        });
    }

    @Override
    public StringByteReader of(final StringColumn column) {
        return (StringByteReader) readers.computeIfAbsent(column, col -> {
            var bytes = readAllBytesOf(column);
            return new StringByteReader(bytes);
        });
    }

    private byte[] readAllBytesOf(final Column column) {
        var columnFile = root.resolve(column.type().name()).resolve(column.name());
        try {
            return Files.readAllBytes(columnFile);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private abstract static class ByteReader implements ColumnReader {

        protected final byte[] bytes;
        private int index = -1;

        private ByteReader(final byte[] bytes) {
            this.bytes = bytes;
        }

        public void reset() {
            index = -1;
        }

        @Override
        public boolean isPresent() {
            return bytes[index] == Byte.MAX_VALUE;
        }

        protected abstract int currentSize();

        protected int start() {
            return index + Byte.BYTES;
        }

        private boolean hasNext() {
            if (index == -1) {
                return bytes.length > 0;
            }
            if (isPresent()) {
                return index + Byte.BYTES + currentSize() < bytes.length;
            }
            return index + Byte.BYTES < bytes.length;
        }

        private void next() {
            if (index == -1) {
                index = 0;
                return;
            }
            if (isPresent()) {
                index += currentSize();
            }
            index += Byte.BYTES;
        }
    }

    public static final class BooleanByteReader extends ByteReader implements BooleanColumnReader {

        private BooleanByteReader(final byte[] bytes) {
            super(bytes);
        }

        @Override
        public boolean get() {
            return bytes[start()] == Byte.MAX_VALUE;
        }

        @Override
        protected int currentSize() {
            return Byte.BYTES;
        }
    }

    public static final class DoubleByteReader extends ByteReader implements DoubleColumnReader {

        private final ByteBuffer buffer = ByteBuffer.wrap(new byte[Double.BYTES]);

        private DoubleByteReader(final byte[] bytes) {
            super(bytes);
        }

        @Override
        public double get() {
            return buffer.clear().put(bytes, start(), Double.BYTES).flip().getDouble();
        }

        @Override
        protected int currentSize() {
            return Double.BYTES;
        }
    }

    public static final class IdByteReader extends ByteReader implements IdColumnReader {

        private final ByteBuffer buffer = ByteBuffer.wrap(new byte[Integer.BYTES]);

        private IdByteReader(final byte[] bytes) {
            super(bytes);
        }

        @Override
        public byte[] get() {
            int length = buffer.clear().put(bytes, start(), Integer.BYTES).flip().getInt();
            int start = start() + Integer.BYTES;
            return Arrays.copyOfRange(bytes, start, start + length);
        }

        @Override
        protected int currentSize() {
            int length = buffer.clear().put(bytes, start(), Integer.BYTES).flip().getInt();
            return Integer.BYTES + length;
        }
    }

    public static final class LongByteReader extends ByteReader implements LongColumnReader {

        private final ByteBuffer buffer = ByteBuffer.wrap(new byte[Long.BYTES]);

        private LongByteReader(final byte[] bytes) {
            super(bytes);
        }

        @Override
        public long get() {
            return buffer.clear().put(bytes, start(), Long.BYTES).flip().getLong();
        }

        @Override
        protected int currentSize() {
            return Long.BYTES;
        }
    }

    public static final class StringByteReader extends ByteReader implements StringColumnReader {

        private final ByteBuffer buffer = ByteBuffer.wrap(new byte[Integer.BYTES]);

        private StringByteReader(final byte[] bytes) {
            super(bytes);
        }

        @Override
        public String get() {
            int length = buffer.clear().put(bytes, start(), Integer.BYTES).flip().getInt();
            int start = start() + Integer.BYTES;
            return new String(bytes, start, length, StandardCharsets.UTF_8);
        }

        @Override
        protected int currentSize() {
            int length = buffer.clear().put(bytes, start(), Integer.BYTES).flip().getInt();
            return Integer.BYTES + length;
        }
    }

    @Override
    public boolean hasNext() {
        if (!consumed) {
            return hasNext;
        }
        consumed = false;
        hasNext = false;
        for (ByteReader reader : readers.values()) {
            if (reader.hasNext()) {
                reader.next();
                hasNext = true;
            }
        }
        return hasNext;
    }

    @Override
    public void next() {
        if (!consumed && !hasNext) {
            throw new NoSuchElementException("No next value");
        }
        if (consumed && !hasNext()) {
            throw new NoSuchElementException("No next value");
        }
        assert !consumed;
        consumed = true;
    }

    @Override
    public void close() {
        readers.clear();
    }

    public void reset() {
        readers.values().forEach(ByteReader::reset);
        consumed = true;
        hasNext = false;
    }
}
