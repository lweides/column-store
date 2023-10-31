package column.store.parquet.write;

import column.store.api.column.Column;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;

class IdWriterTest extends BaseWriterTest<IdWriter, byte[]> {

  @Override
  protected IdWriter writer() {
    return new IdWriter(Column.forId("an-id"), 0);
  }

  @Override
  protected byte[] write(final IdWriter writer) {
    var value = new byte[] { 1, 2, 3, 4 };
    writer.write(value);
    return value;
  }

  @Override
  protected void verifyValueWritten(final byte[] value, final RecordConsumer recordConsumer) {
    verify(recordConsumer, times(1)).addBinary(Binary.fromConstantByteArray(value));
  }
}
