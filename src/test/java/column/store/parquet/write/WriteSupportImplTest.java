package column.store.parquet.write;

import column.store.api.column.Column;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Arrays;

import org.apache.parquet.io.api.RecordConsumer;
import org.junit.jupiter.api.Test;

class WriteSupportImplTest {

  @Test
  void createsCorrectSchemaFromColumns() {
    var writers = new BaseWriter[] {
            new BooleanWriter(Column.forBoolean("a-boolean"), 0),
            new DoubleWriter(Column.forDouble("a-double"), 1),
            new IdWriter(Column.forId("an-id"), 2),
            new LongWriter(Column.forLong("a-long"), 3),
            new StringWriter(Column.forString("a-string"), 4)
    };
    var toTest = new WriteSupportImpl(writers);
    var context = toTest.init(mock());

    assertThat(context.getExtraMetaData()).isEmpty();

    var expectedSchema = """
            message record {
                optional boolean a-boolean;
                optional double a-double;
                optional binary an-id;
                optional int64 a-long;
                optional binary a-string;
            }
            """;
    assertThat(context.getSchema().toString()).isEqualToIgnoringWhitespace(expectedSchema);
  }

  @Test
  void writesValuesToConsumer() {
    var booleanWriter = new BooleanWriter(Column.forBoolean("a-boolean"), 0);
    var doubleWriter = new DoubleWriter(Column.forDouble("a-double"), 1);
    var idWriter = new IdWriter(Column.forId("an-id"), 2);
    var longWriter = new LongWriter(Column.forLong("a-long"), 3);
    var stringWriter = new StringWriter(Column.forString("a-string"), 4);

    var toTest = new WriteSupportImpl(new BaseWriter[] { booleanWriter, doubleWriter, idWriter, longWriter, stringWriter });

    booleanWriter.write(true);
    doubleWriter.write(3.14);
    idWriter.write(new byte[] { 1, 2, 3, 4 });
    longWriter.write(73L);
    stringWriter.write("The quick brown fox jumps over the lazy dog");

    var recordConsumer = mock(RecordConsumer.class);
    toTest.prepareForWrite(recordConsumer);

    toTest.write(mock());

    verify(recordConsumer, times(1)).startMessage();
    verify(recordConsumer, times(5)).startField(any(), anyInt());
    verify(recordConsumer, times(1)).addBoolean(true);
    verify(recordConsumer, times(1)).addDouble(3.14);
    verify(recordConsumer, times(2)).addBinary(argThat(value ->
                    value.toStringUsingUTF8().equals("The quick brown fox jumps over the lazy dog") ||
                    Arrays.equals(value.getBytes(), new byte[] { 1, 2, 3, 4 })));
    verify(recordConsumer, times(1)).addLong(73L);
    verify(recordConsumer, times(5)).endField(any(), anyInt());
    verify(recordConsumer, times(1)).endMessage();
  }
}