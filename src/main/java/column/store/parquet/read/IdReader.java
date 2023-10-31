package column.store.parquet.read;

import column.store.api.read.IdColumnReader;

import org.apache.parquet.io.api.Binary;

class IdReader extends BaseReader implements IdColumnReader {

  private byte[] value;

  @Override
  public byte[] get() {
    ensureIsPresent();
    return value;
  }

  @Override
  public void addBinary(final Binary value) {
    markAsPresent();
    this.value = value.getBytes();
  }
}
