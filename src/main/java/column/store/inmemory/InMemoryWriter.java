package column.store.inmemory;

import column.store.api.column.*;
import column.store.api.write.*;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

import static column.store.util.Conditions.checkState;

public class InMemoryWriter implements Writer {

    private final Path root;
    private final Map<Column, ColumnWriter> writers;
    private final Map<Column, FileChannel> channels;

    public InMemoryWriter(final Path root) {
        this.root = root;
        this.writers = new HashMap<>();
        this.channels = new HashMap<>();
    }

    @Override
    public BooleanColumnWriter of(final BooleanColumn column) {
        return (BooleanColumnWriter) writers.computeIfAbsent(column, col -> {
            var channel = openChannelFor(column); // is added to channels
            var buffer = ByteBuffer.allocateDirect(Byte.BYTES + Byte.BYTES);
            return new BooleanColumnWriter() {
               @Override
               public void write(final boolean value) {
                   buffer.put(Byte.MAX_VALUE);
                   buffer.put(value ? Byte.MAX_VALUE : Byte.MIN_VALUE);
                   InMemoryWriter.write(channel, buffer);
               }

               @Override
               public void writeNull() {
                   buffer.put(Byte.MIN_VALUE);
                   InMemoryWriter.write(channel, buffer);
               }
           };
        });
    }

    @Override
    public DoubleColumnWriter of(final DoubleColumn column) {
        return (DoubleColumnWriter) writers.computeIfAbsent(column, col -> {
            var channel = openChannelFor(column); // is added to channels
            var buffer = ByteBuffer.allocateDirect(Byte.BYTES + Double.BYTES);
            return new DoubleColumnWriter() {
                @Override
                public void write(final double value) {
                    buffer.put(Byte.MAX_VALUE);
                    buffer.putDouble(value);
                    InMemoryWriter.write(channel, buffer);
                }

                @Override
                public void writeNull() {
                    buffer.put(Byte.MIN_VALUE);
                    InMemoryWriter.write(channel, buffer);
                }
            };
        });
    }

    @Override
    public IdColumnWriter of(final IdColumn column) {
        return (IdColumnWriter) writers.computeIfAbsent(column, col -> {
            var channel = openChannelFor(column); // is added to channels
            return new IdColumnWriter() {
                @Override
                public void write(final byte[] value) {
                    var buffer = ByteBuffer.allocateDirect(Byte.BYTES + Integer.BYTES + value.length);
                    buffer.put(Byte.MAX_VALUE);
                    buffer.putInt(value.length);
                    buffer.put(value);
                    InMemoryWriter.write(channel, buffer);
                }

                @Override
                public void writeNull() {
                    var buffer = ByteBuffer.allocateDirect(Byte.BYTES);
                    buffer.put(Byte.MIN_VALUE);
                    InMemoryWriter.write(channel, buffer);
                }
            };
        });
    }

    @Override
    public LongColumnWriter of(final LongColumn column) {
        return (LongColumnWriter) writers.computeIfAbsent(column, col -> {
            var channel = openChannelFor(column); // is added to channels
            var buffer = ByteBuffer.allocateDirect(Byte.BYTES + Long.BYTES);
            return new LongColumnWriter() {
                @Override
                public void write(final long value) {
                    buffer.put(Byte.MAX_VALUE);
                    buffer.putLong(value);
                    InMemoryWriter.write(channel, buffer);
                }

                @Override
                public void writeNull() {
                    buffer.put(Byte.MIN_VALUE);
                    InMemoryWriter.write(channel, buffer);
                }
            };
        });
    }

    @Override
    public StringColumnWriter of(final StringColumn column) {
        return (StringColumnWriter) writers.computeIfAbsent(column, col -> {
            var channel = openChannelFor(column); // is added to channels
            return new StringColumnWriter() {
                @Override
                public void write(final String value) {
                    var bytes = value.getBytes(StandardCharsets.UTF_8);
                    var buffer = ByteBuffer.allocateDirect(Byte.BYTES + Integer.BYTES + bytes.length);
                    buffer.put(Byte.MAX_VALUE);
                    buffer.putInt(bytes.length);
                    buffer.put(bytes);
                    InMemoryWriter.write(channel, buffer);
                }

                @Override
                public void writeNull() {
                    var buffer = ByteBuffer.allocateDirect(Byte.BYTES);
                    buffer.put(Byte.MIN_VALUE);
                    InMemoryWriter.write(channel, buffer);
                }
            };
        });
    }

    private FileChannel openChannelFor(final Column column) {
        var columnFile = root.resolve(column.type().name()).resolve(column.name());
        try {
            Files.createDirectories(columnFile.getParent()); // does not throw if directory already exists
            Files.createFile(columnFile);
            var channel = FileChannel.open(columnFile, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            checkState(channels.put(column, channel) == null, "Channel has already been opened");
            return channel;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void write(final FileChannel channel, final ByteBuffer content) {
        content.flip();
        try {
            channel.write(content);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            content.clear();
        }
    }

    @Override
    public void next() {
        // no-op
    }

    @Override
    public void flush() throws IOException {
        try {
            channels.values().forEach(channel -> {
                try {
                    channel.force(true);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        try {
            channels.values().forEach(channel -> {
                try {
                    channel.close();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }
}
