package column.store.parquet.write;

import column.store.api.column.Column;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;

class StringWriterTest extends BaseWriterTest<StringWriter, String> {

  @Override
  protected StringWriter writer() {
    return new StringWriter(Column.forString("a-string"), 0);
  }

  @Override
  protected String write(final StringWriter writer) {
    var value = "The quick brown fox jumps over the lazy dog";
    writer.write(value);
    return value;
  }

  @Override
  protected void verifyValueWritten(final String value, final RecordConsumer recordConsumer) {
    verify(recordConsumer, times(1)).addBinary(Binary.fromString(value));
  }
}
