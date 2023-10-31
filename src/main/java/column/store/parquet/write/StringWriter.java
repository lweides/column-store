package column.store.parquet.write;

import column.store.api.column.StringColumn;
import column.store.api.write.StringColumnWriter;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;

class StringWriter extends BaseWriter implements StringColumnWriter {

  private String value;

  StringWriter(final StringColumn column, final int field) {
    super(column, field);
  }

  @Override
  public void write(final String value) {
    this.value = value;
    this.isPresent = true;
  }

  @Override
  protected void add(final RecordConsumer recordConsumer) {
    recordConsumer.addBinary(Binary.fromString(value));
  }
}
