package column.store.parquet.write;

import column.store.api.column.Column;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.parquet.io.api.RecordConsumer;

class DoubleWriterTest extends BaseWriterTest<DoubleWriter, Double> {

  @Override
  protected DoubleWriter writer() {
    return new DoubleWriter(Column.forDouble("a-double"), 0);
  }

  @Override
  protected Double write(final DoubleWriter writer) {
    double value = 3.14;
    writer.write(value);
    return value;
  }

  @Override
  protected void verifyValueWritten(final Double value, final RecordConsumer recordConsumer) {
    verify(recordConsumer, times(1)).addDouble(value);
  }
}
