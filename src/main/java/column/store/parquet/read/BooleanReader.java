package column.store.parquet.read;

import column.store.api.read.BooleanColumnReader;

class BooleanReader extends BaseReader implements BooleanColumnReader {

  private boolean value;

  @Override
  public boolean get() {
    ensureIsPresent();
    return value;
  }

  @Override
  public void addBoolean(final boolean value) {
    markAsPresent();
    this.value = value;
  }
}
