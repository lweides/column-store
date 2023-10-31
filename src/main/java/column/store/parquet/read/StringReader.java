package column.store.parquet.read;

import column.store.api.read.StringColumnReader;

import org.apache.parquet.io.api.Binary;

class StringReader extends BaseReader implements StringColumnReader {

  private Binary value;

  @Override
  public String get() {
    ensureIsPresent();
    return value.toStringUsingUTF8();
  }

  @Override
  public void addBinary(final Binary value) {
    markAsPresent();
    this.value = value;
  }
}
