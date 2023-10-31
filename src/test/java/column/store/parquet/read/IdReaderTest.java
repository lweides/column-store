package column.store.parquet.read;

import org.apache.parquet.io.api.Binary;

class IdReaderTest extends BaseReaderTest<IdReader, byte[]> {

  @Override
  protected int unsupportedOperations() {
    return 3;
  }

  @Override
  protected IdReader reader() {
    return new IdReader();
  }

  @Override
  protected byte[] add(final IdReader reader) {
    var id = new byte[] { 1, 2, 3, 4 };
    reader.addBinary(Binary.fromConstantByteArray(id));
    return id;
  }

  @Override
  protected byte[] get(final IdReader reader) {
    return reader.get();
  }
}
