package column.store.csv.read;

import column.store.api.column.*;
import column.store.api.query.Filter;
import column.store.api.query.Query;
import column.store.api.read.*;
import column.store.csv.CSVFile;
import column.store.util.EvalFilterUtil;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.SerializationUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;


public class CSVReader implements Reader {

    private static final Base64.Decoder BASE_64 = Base64.getDecoder();

    // MEMORY-MAPPING SECTION
    private static final int ALLOCATED_MEMORY = 268_435_456; // ~ 256 MB
    private int memoryThreshold = -1;
    private int memoryLeft = ALLOCATED_MEMORY;
    private MappedByteBuffer mmap;
    private int offset = 0;
    private int numOfStoredRecords = 0;
    private int numOfReadRecord = 0;
    private final ByteBuffer recordSizeBuffer = ByteBuffer.allocate(Integer.BYTES);

    // QUERY/RECORD SECTION
    private Iterable<Filter> filters = new ArrayList<>();
    private final Map<String, Integer> header = new HashMap<>();
    private boolean isAllOf = true;
    private CSVParser csvParser;
    private CSVRecord currentRecord = null;

    @Override
    public void query(final Query query) throws IOException {
        // reset previous query settings/result
        isAllOf = query.type().equals(Query.QueryType.ALL_OF);
        memoryLeft = ALLOCATED_MEMORY;
        memoryThreshold = -1;
        filters = query.filters();
        header.clear();
        close();

        csvParser = new CSVFile(query.filePath()).getRecords();

        header.putAll(csvParser.getHeaderMap());
        fetchNextRecords();
    }

    private void fetchNextRecords() throws IOException {
        if (mmap != null) {
            mmap.clear();
        } else {
            mmap = new RandomAccessFile(File.createTempFile("temp", ".dat"), "rw")
                    .getChannel()
                    .map(FileChannel.MapMode.READ_WRITE, 0, ALLOCATED_MEMORY);
        }
        numOfStoredRecords = 0;
        numOfReadRecord = 0;
        offset = 0;
        if (isAllOf) {
            allOfQuery();
        } else {
            atLeastOne();
        }
    }

    private void allOfQuery() {
        for (CSVRecord record : csvParser) {
            if (evalAll(record)) {
                memoryLeft -= putRecord(record);
                if (memoryLeft < memoryThreshold) {
                    return;
                }
            }
        }
    }

    private boolean evalAll(final CSVRecord record) {
        for (Filter filter : filters) {
            if (!EvalFilterUtil.eval(record.get(filter.column().name()), filter)) {
                return false;
            }
        }

        return true;
    }

    private int putRecord(final CSVRecord record) {
        byte[] recordBytes = SerializationUtils.serialize(record);
        byte[] recordSize = recordSizeBuffer.clear()
                .putInt(recordBytes.length)
                .array();
        mmap.put(recordSize, 0, Integer.BYTES);
        mmap.put(recordBytes, 0, recordBytes.length);
        numOfStoredRecords += 1;


        if (memoryThreshold == -1) {
            memoryThreshold = (Integer.BYTES + recordBytes.length) * 2;
        }
        return Integer.BYTES + recordBytes.length;
    }

    private void getNextRecord() {
        byte[] recordSizeBytes = new byte[Integer.BYTES];
        mmap.get(offset, recordSizeBytes);
        offset += Integer.BYTES;
        int recordSize = ByteBuffer.allocate(Integer.BYTES)
                .put(recordSizeBytes)
                .rewind()
                .getInt();

        byte[] recordBytes = new byte[recordSize];
        mmap.get(offset, recordBytes);
        offset += recordSize;

        currentRecord = SerializationUtils.deserialize(recordBytes);
        numOfReadRecord += 1;
    }

    private void atLeastOne() {
        for (CSVRecord record : csvParser) {
            if (evalAny(record)) {
                memoryLeft -= putRecord(record);
                if (memoryLeft < memoryThreshold) {
                    return;
                }
            }
        }
    }

    private boolean evalAny(final CSVRecord record) {
        for (Filter filter : filters) {
            if (EvalFilterUtil.eval(record.get(filter.column().name()), filter)) {
                return true;
            }
        }

        return false;
    }

    private boolean isPresent(final Column column) {
        if (!header.containsKey(column.name())) {
            throw new NoSuchColumnException(column);
        }

        return currentRecord != null && !currentRecord.get(header.get(column.name())).isEmpty();
    }

    private String getColumnValue(final Column column) throws NullPointerException {
        if (!isPresent(column)) {
            throw new NoSuchElementException("Current value is null");
        }
        return currentRecord.get(header.get(column.name()));
    }

    @Override
    public BooleanColumnReader of(final BooleanColumn column) {
        return new BooleanColumnReader() {
            @Override
            public boolean get() {
                return Boolean.parseBoolean(getColumnValue(column));
            }

            @Override
            public boolean isPresent() {
                return CSVReader.this.isPresent(column);
            }
        };
    }

    @Override
    public DoubleColumnReader of(final DoubleColumn column) {
        return new DoubleColumnReader() {
            @Override
            public double get() {
                return Double.parseDouble(getColumnValue(column));
            }

            @Override
            public boolean isPresent() {
                return CSVReader.this.isPresent(column);
            }
        };
    }

    @Override
    public IdColumnReader of(final IdColumn column) {
        return new IdColumnReader() {
            @Override
            public byte[] get() {
                var encodedBytes = getColumnValue(column).getBytes(StandardCharsets.UTF_8);
                return BASE_64.decode(encodedBytes);
            }

            @Override
            public boolean isPresent() {
                return CSVReader.this.isPresent(column);
            }
        };
    }

    @Override
    public LongColumnReader of(final LongColumn column) {
        return new LongColumnReader() {
            @Override
            public long get() {
                return Long.parseLong(getColumnValue(column));
            }

            @Override
            public boolean isPresent() {
                return CSVReader.this.isPresent(column);
            }
        };
    }

    @Override
    public StringColumnReader of(final StringColumn column) {
        return new StringColumnReader() {
            @Override
            public String get() {
                return getColumnValue(column);
            }

            @Override
            public boolean isPresent() {
                return CSVReader.this.isPresent(column);
            }
        };
    }

    @Override
    public boolean hasNext() {
        return numOfReadRecord < numOfStoredRecords || (csvParser != null && csvParser.iterator().hasNext());
    }

    @Override
    public void next() throws IOException {
        if (numOfReadRecord < numOfStoredRecords) {
            getNextRecord();
        } else if (hasNext()) {
            fetchNextRecords();
        } else {
            throw new NoSuchElementException("No next value");
        }
    }

    @Override
    public void close() throws IOException {
        if (csvParser != null) {
            csvParser.close();
        }
    }
}
