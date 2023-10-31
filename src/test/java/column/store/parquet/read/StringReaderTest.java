package column.store.parquet.read;

import org.apache.parquet.io.api.Binary;

class StringReaderTest extends BaseReaderTest<StringReader, String> {

  @Override
  protected int unsupportedOperations() {
    return 3;
  }

  @Override
  protected StringReader reader() {
    return new StringReader();
  }

  @Override
  protected String add(final StringReader reader) {
    var value = "The quick brown fox jumps over the lazy dog";
    reader.addBinary(Binary.fromString(value));
    return value;
  }

  @Override
  protected String get(final StringReader reader) {
    return reader.get();
  }
}
