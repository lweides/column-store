package column.store.csv.write;

import column.store.api.column.*;
import column.store.api.write.*;
import column.store.api.write.Writer;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;

public class CSVWriter implements Writer {


    private List<String[]> recordsToWrite;
    private final Map<String, Integer> headers;
    private String[] currentRecord;
    private final Path csvFilePath;
    private final int maxInMemoryRecords = 500_000;

    public CSVWriter(final Path csvDestPath, final List<Column> columns) throws IOException {
        this.csvFilePath = csvDestPath;

        recordsToWrite = new ArrayList<>();
        headers = new LinkedHashMap<>();
        setUpWriter(columns);
        initCsvFile();
    }

    public CSVWriter(final Path existingCsv) throws IOException {
        this.csvFilePath = existingCsv;

        recordsToWrite = new ArrayList<>();
        headers = new LinkedHashMap<>();
        setUpWriter(existingCsv);
    }

    private void setUpWriter(final List<Column> columns) throws IOException {
        for (int i = 0; i < columns.size(); i++) {
            headers.put(columns.get(i).name().toLowerCase(Locale.ROOT), i);
        }
        initNewRecord();
    }


    private void setUpWriter(final Path csvFilePath) throws IOException {
        String[] headerValues = new BufferedReader(new FileReader(csvFilePath.toString())).readLine().split(",");
        for (int i = 0; i < headerValues.length; i++) {
            headers.put(headerValues[i].toLowerCase(Locale.ROOT), i);
        }
        initNewRecord();
    }

    private void initCsvFile() throws IOException {
        try (FileWriter wr = new FileWriter(csvFilePath.toString(), false)) {
            wr.write(String.join(",", headers.keySet()));
        }
    }

    private void initNewRecord() {
        currentRecord = new String[headers.size()];
        Arrays.fill(currentRecord, "");
    }

    private String prepareStringForWriting(final String value) {
        return "\"" +
                value.replace("\"", "'")
                        .replace("\b", "\\b")
                        .replace("\f", "\\f")
                        .replace("\n", "\\n")
                        .replace("\r", "\\r")
                        .replace("\t", "\\t") +
                "\"";
    }

    public void writeNull(final String columnName) {
        currentRecord[headers.get(columnName)] = "";
    }

    private void ensureColumnExists(final Column column) throws NoSuchColumnException {
        if (!headers.containsKey(column.name())) {
            throw new NoSuchColumnException(column);
        }
    }

    @Override
    public BooleanColumnWriter of(final BooleanColumn column)  {
        ensureColumnExists(column);

        return new BooleanColumnWriter() {
            @Override
            public void write(final boolean value) {
                currentRecord[headers.get(column.name())] = String.valueOf(value);
            }

            @Override
            public void writeNull() {
                CSVWriter.this.writeNull(column.name());
            }
        };
    }

    @Override
    public DoubleColumnWriter of(final DoubleColumn column) {
        ensureColumnExists(column);

        return new DoubleColumnWriter() {
            @Override
            public void write(final double value) {
                currentRecord[headers.get(column.name())] = String.valueOf(value);
            }

            @Override
            public void writeNull() {
                CSVWriter.this.writeNull(column.name());
            }
        };
    }

    @Override
    public IdColumnWriter of(final IdColumn column) {
        ensureColumnExists(column);

        return new IdColumnWriter() {
            @Override
            public void write(final byte[] value) {
                currentRecord[headers.get(column.name())] = prepareStringForWriting(new String(value, StandardCharsets.UTF_8));
            }

            @Override
            public void writeNull() {
                CSVWriter.this.writeNull(column.name());
            }
        };
    }

    @Override
    public LongColumnWriter of(final LongColumn column) {
        ensureColumnExists(column);

        return new LongColumnWriter() {
            @Override
            public void write(final long value) {
                currentRecord[headers.get(column.name())] = String.valueOf(value);
            }

            @Override
            public void writeNull() {
                CSVWriter.this.writeNull(column.name());
            }
        };
    }

    @Override
    public StringColumnWriter of(final StringColumn column) {
        ensureColumnExists(column);

        return new StringColumnWriter() {
            @Override
            public void write(final String value) {
                currentRecord[headers.get(column.name())] = prepareStringForWriting(value);
            }

            @Override
            public void writeNull() {
                CSVWriter.this.writeNull(column.name());
            }
        };
    }

    @Override
    public void next() throws IOException {
        recordsToWrite.add(currentRecord);
        initNewRecord();

        if (recordsToWrite.size() >= maxInMemoryRecords) {
            flush();
        }
    }

    @Override
    public void flush() throws IOException {
        try (FileWriter wr = new FileWriter(csvFilePath.toString(), true)) {
            var newRecords = recordsToWrite.stream()
                    .map(strArr -> String.join(",", strArr))
                    .toList();
            wr.write("\n" + String.join("\n", newRecords));
        }

        recordsToWrite = new ArrayList<>();
        initNewRecord();
    }

    @Override
    public void close() throws IOException {
        if (!recordsToWrite.isEmpty()) {
            flush();
        }
    }
}
