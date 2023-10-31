package column.store.parquet.read;

import column.store.api.read.ColumnReader;

import java.util.NoSuchElementException;

import org.apache.parquet.io.api.PrimitiveConverter;

abstract class BaseReader extends PrimitiveConverter implements ColumnReader {

  private boolean isPresent;

  @Override
  public boolean isPresent() {
    return isPresent;
  }

  protected void markAsPresent() {
    isPresent = true;
  }

  void reset() {
    isPresent = false;
  }

  protected void ensureIsPresent() {
    if (isNull()) {
      throw new NoSuchElementException("Current value is null");
    }
  }
}
