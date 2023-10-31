package column.store.parquet.read;

class LongReaderTest extends BaseReaderTest<LongReader, Long> {

  @Override
  protected LongReader reader() {
    return new LongReader();
  }

  @Override
  protected Long add(final LongReader reader) {
    reader.addLong(73L);
    return 73L;
  }

  @Override
  protected Long get(final LongReader reader) {
    return reader.get();
  }
}
