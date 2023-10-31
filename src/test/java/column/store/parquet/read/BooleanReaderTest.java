package column.store.parquet.read;

class BooleanReaderTest extends BaseReaderTest<BooleanReader, Boolean> {

  @Override
  protected BooleanReader reader() {
    return new BooleanReader();
  }

  @Override
  protected Boolean add(final BooleanReader reader) {
    reader.addBoolean(true);
    return true;
  }

  @Override
  protected Boolean get(final BooleanReader reader) {
    return reader.get();
  }
}
