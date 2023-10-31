package column.store.parquet.read;

import column.store.api.read.LongColumnReader;

class LongReader extends BaseReader implements LongColumnReader {

  private long value;

  @Override
  public long get() {
    ensureIsPresent();
    return value;
  }

  @Override
  public void addLong(final long value) {
    markAsPresent();
    this.value = value;
  }
}
