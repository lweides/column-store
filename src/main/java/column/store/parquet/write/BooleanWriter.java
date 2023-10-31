package column.store.parquet.write;

import column.store.api.column.BooleanColumn;
import column.store.api.write.BooleanColumnWriter;

import org.apache.parquet.io.api.RecordConsumer;

class BooleanWriter extends BaseWriter implements BooleanColumnWriter {

  private boolean value;

  BooleanWriter(final BooleanColumn column, final int field) {
    super(column, field);
  }

  @Override
  public void write(final boolean value) {
    this.value = value;
    this.isPresent = true;
  }

  @Override
  protected void add(final RecordConsumer recordConsumer) {
    recordConsumer.addBoolean(value);
  }
}
