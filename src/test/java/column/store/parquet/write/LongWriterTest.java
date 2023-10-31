package column.store.parquet.write;

import column.store.api.column.Column;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.parquet.io.api.RecordConsumer;

class LongWriterTest extends BaseWriterTest<LongWriter, Long> {

  @Override
  protected LongWriter writer() {
    return new LongWriter(Column.forLong("a-long"), 0);
  }

  @Override
  protected Long write(final LongWriter writer) {
    long value = 73L;
    writer.write(value);
    return value;
  }

  @Override
  protected void verifyValueWritten(final Long value, final RecordConsumer recordConsumer) {
    verify(recordConsumer, times(1)).addLong(value);
  }
}
