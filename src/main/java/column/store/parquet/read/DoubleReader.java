package column.store.parquet.read;

import column.store.api.read.DoubleColumnReader;

class DoubleReader extends BaseReader implements DoubleColumnReader {

  private double value;

  @Override
  public double get() {
    ensureIsPresent();
    return value;
  }

  @Override
  public void addDouble(final double value) {
    markAsPresent();
    this.value = value;
  }
}
