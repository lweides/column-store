package column.store.parquet.write;

import column.store.api.column.IdColumn;
import column.store.api.write.IdColumnWriter;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;

class IdWriter extends BaseWriter implements IdColumnWriter {

  private byte[] value;

  IdWriter(final IdColumn column, final int field) {
    super(column, field);
  }

  @Override
  public void write(final byte[] value) {
    this.value = value;
    this.isPresent = true;
  }

  @Override
  protected void add(final RecordConsumer recordConsumer) {
    recordConsumer.addBinary(Binary.fromConstantByteArray(value));
  }
}
