package column.store.parquet.write;

import column.store.api.column.LongColumn;
import column.store.api.write.LongColumnWriter;

import org.apache.parquet.io.api.RecordConsumer;

class LongWriter extends BaseWriter implements LongColumnWriter {

  private long value;

  LongWriter(final LongColumn column, final int field) {
    super(column, field);
  }

  @Override
  public void write(final long value) {
    this.value = value;
    this.isPresent = true;
  }

  @Override
  protected void add(final RecordConsumer recordConsumer) {
    recordConsumer.addLong(value);
  }
}
