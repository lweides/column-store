package column.store.parquet.write;

import column.store.api.column.Column;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.parquet.io.api.RecordConsumer;

class BooleanWriterTest extends BaseWriterTest<BooleanWriter, Boolean> {

  @Override
  protected BooleanWriter writer() {
    return new BooleanWriter(Column.forBoolean("a-boolean"), 0);
  }

  @Override
  protected Boolean write(final BooleanWriter writer) {
    boolean value = true;
    writer.write(value);
    return value;
  }

  @Override
  protected void verifyValueWritten(final Boolean value, final RecordConsumer recordConsumer) {
    verify(recordConsumer, times(1)).addBoolean(value);
  }
}
