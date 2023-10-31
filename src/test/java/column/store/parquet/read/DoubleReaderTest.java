package column.store.parquet.read;

class DoubleReaderTest extends BaseReaderTest<DoubleReader, Double> {

  @Override
  protected DoubleReader reader() {
    return new DoubleReader();
  }

  @Override
  protected Double add(final DoubleReader reader) {
    reader.addDouble(3.14);
    return 3.14;
  }

  @Override
  protected Double get(final DoubleReader reader) {
    return reader.get();
  }
}
