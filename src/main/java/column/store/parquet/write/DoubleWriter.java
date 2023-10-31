package column.store.parquet.write;

import column.store.api.column.DoubleColumn;
import column.store.api.write.DoubleColumnWriter;

import org.apache.parquet.io.api.RecordConsumer;

class DoubleWriter extends BaseWriter implements DoubleColumnWriter {

  private double value;

  DoubleWriter(final DoubleColumn column, final int field) {
    super(column, field);
  }

  @Override
  public void write(final double value) {
    this.value = value;
    this.isPresent = true;
  }

  @Override
  protected void add(final RecordConsumer recordConsumer) {
    recordConsumer.addDouble(value);
  }
}
