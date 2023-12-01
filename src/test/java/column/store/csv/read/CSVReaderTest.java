package column.store.csv.read;

import column.store.api.column.*;
import column.store.api.query.Query;
import column.store.api.read.DoubleColumnReader;
import column.store.api.read.IdColumnReader;
import column.store.api.read.StringColumnReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

import static column.store.api.query.Filter.*;
import static org.assertj.core.api.Assertions.*;
public class CSVReaderTest {

    private CSVReader csvReader;
    private Query.Builder queryBuilder;
    private static final Path CSVFILE_PATH = Path.of("src/test/resources/TLC_taxi.csv");
    private final DoubleColumn tripDistance = Column.forDouble("trip_distance");
    private final IdColumn vendorId = Column.forId("VendorID");
    private final StringColumn pickupTime = Column.forString("tpep_pickup_datetime");

    @BeforeEach
    public void setup() {
        csvReader = new CSVReader();
        queryBuilder = Query.from(CSVFILE_PATH);
    }

    @Test
    public void readNonExistingColumn() throws IOException {
        var query = queryBuilder.select(vendorId).allOf();
        csvReader.query(query);

        var nonExistingColumn = Column.forId("taxi_driver_id");
        IdColumnReader idColumnReader = csvReader.of(nonExistingColumn);

        assertThatThrownBy(idColumnReader::isPresent).isInstanceOf(NoSuchColumnException.class)
                .hasMessage("Column taxi_driver_id of type ID not found");
    }

    @Test
    public void readAllOfQuery() throws IOException {
        var moreThanTenMilesFilter = whereDouble(tripDistance).isGreaterThan(10.0);
        var pickupTimeFilter = whereString(pickupTime).endsWith("00:27:12");

        var allOfQuery = queryBuilder.select(tripDistance, pickupTime)
                .filter(moreThanTenMilesFilter)
                .filter(pickupTimeFilter)
                .allOf();
        csvReader.query(allOfQuery);

        DoubleColumnReader tripDistanceReader = csvReader.of(tripDistance);
        StringColumnReader pickupTimeReader = csvReader.of(pickupTime);

        assertThat(csvReader.hasNext())
                .as("Expected query to return at least one result")
                .isTrue();

        csvReader.next();

        assertThat(tripDistanceReader.isPresent())
                .as("Expected a value to be present for %s", tripDistance.name())
                .isTrue();

        assertThat(pickupTimeReader.isPresent())
                .as("Expected a value to be present for %s", pickupTime.name())
                .isTrue();

        assertThat(tripDistanceReader.get()).isGreaterThan(10.0);
        assertThat(pickupTimeReader.get()).endsWith("00:27:12");
    }
}
