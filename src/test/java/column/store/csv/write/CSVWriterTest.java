package column.store.csv.write;

import column.store.api.column.Column;
import column.store.api.column.DoubleColumn;
import column.store.api.column.IdColumn;
import column.store.api.column.StringColumn;
import column.store.api.query.Filter;
import column.store.api.query.Query;
import column.store.csv.read.CSVReader;
import org.junit.jupiter.api.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import static org.assertj.core.api.Assertions.*;

public class CSVWriterTest {

    private CSVWriter csvWriter;
    private CSVReader csvReader;
    private static final Path CSV_FILE_PATH = Paths.get("src/test/resources/TLC_taxi.csv");
    private static final Path BACKUP = Path.of(CSV_FILE_PATH.toString().replace("TLC_taxi", "TLC_taxi_backup"));
    private final DoubleColumn tripDistance = Column.forDouble("trip_distance");
    private final IdColumn vendorId = Column.forId("VendorID");
    private final StringColumn pickupTime = Column.forString("tpep_pickup_datetime");


    @BeforeAll
    public static void createBackup() throws IOException {
        Files.copy(CSV_FILE_PATH, BACKUP, StandardCopyOption.REPLACE_EXISTING);
    }

    @AfterAll
    public static void deleteBackup() throws IOException {
        Files.deleteIfExists(BACKUP);
    }

    @BeforeEach
    public void setup() throws IOException {
        csvWriter = new CSVWriter(CSV_FILE_PATH);
        csvReader = new CSVReader();
    }

    @AfterEach
    public void teardown() throws IOException {
        csvReader.close();
        Files.copy(BACKUP, CSV_FILE_PATH, StandardCopyOption.REPLACE_EXISTING);
    }

    @Test
    public void writeToCSV() throws IOException {
        var tripDistanceWriter = csvWriter.of(tripDistance);
        var pickupTimeWriter = csvWriter.of(pickupTime);
        var vendorIdWriter = csvWriter.of(vendorId);

        var newVendorId = "1234".getBytes();
        var query = Query.from(CSV_FILE_PATH)
                .select(tripDistance, pickupTime, vendorId)
                .filter(Filter.whereId(vendorId).is(newVendorId))
                .allOf();

        csvReader.query(query);
        var pickupTimeReader = csvReader.of(pickupTime);
        var tripDistanceReader = csvReader.of(tripDistance);
        var vendorIdReader = csvReader.of(vendorId);

        // verify no record exist for given query
        assertThat(csvReader.hasNext())
                .as("Expected no query record to be present")
                .isFalse();

        // add records
        tripDistanceWriter.write(123);
        pickupTimeWriter.write("2023-20-11 00:43:17");
        vendorIdWriter.write(newVendorId);

        csvWriter.next();
        tripDistanceWriter.write(456);
        pickupTimeWriter.write("2023-20-11 12:34:56");
        vendorIdWriter.write(newVendorId);

        csvWriter.next();
        csvWriter.flush();

        // re-evaluate query
        csvReader.query(query);

        assertThat(csvReader.hasNext())
                .as("Expected query to return at least one result")
                .isTrue();

        csvReader.next();
        assertThat(tripDistanceReader.get()).isEqualTo(123);
        assertThat(pickupTimeReader.get()).isEqualTo("2023-20-11 00:43:17");
        assertThat(vendorIdReader.get()).isEqualTo(newVendorId);

        csvReader.next();
        assertThat(tripDistanceReader.get()).isEqualTo(456);
        assertThat(pickupTimeReader.get()).isEqualTo("2023-20-11 12:34:56");
        assertThat(vendorIdReader.get()).isEqualTo(newVendorId);
    }
}
