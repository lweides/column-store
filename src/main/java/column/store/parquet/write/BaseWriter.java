package column.store.parquet.write;

import column.store.api.column.Column;
import column.store.api.write.ColumnWriter;

import org.apache.parquet.io.api.RecordConsumer;

abstract class BaseWriter implements ColumnWriter {

  private final Column column;
  private final int field;

  protected boolean isPresent;

  protected BaseWriter(final Column column, final int field) {
    this.column = column;
    this.field = field;
  }

  @Override
  public void writeNull() {
    isPresent = false;
  }

  void accept(final RecordConsumer recordConsumer) {
    if (isPresent) {
      recordConsumer.startField(column.name(), field);
      add(recordConsumer);
      isPresent = false;
      recordConsumer.endField(column.name(), field);
    }
  }

  protected abstract void add(RecordConsumer recordConsumer);

  Column column() {
    return column;
  }
}
