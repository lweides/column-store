package column.store.csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

import java.io.*;
import java.nio.file.Path;
import java.util.Locale;

public class CSVFile {
    private final CSVFormat csvFormat;
    private final Path csvFilePath;
    public CSVFile(final Path csvFilePath) throws IOException {
        this.csvFilePath = csvFilePath;
        BufferedReader br = new BufferedReader(new FileReader(csvFilePath.toString()));
        String[] header = br.readLine().toLowerCase(Locale.ROOT).split(",");
        br.close();

        this.csvFormat = CSVFormat.DEFAULT.builder()
                .setHeader(header)
                .setSkipHeaderRecord(true)
                .build();
    }

    public CSVParser getRecords() throws IOException {
        return csvFormat.parse(new FileReader(csvFilePath.toString()));
    }
}
