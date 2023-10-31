package column.store.parquet.read;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RecordMaterializerImplTest {

  private Map<String, BaseReader> readers;
  private RecordMaterializerImpl toTest;

  @BeforeEach
  void setup() {
    var schema = MessageTypeParser.parseMessageType("""
            message record {
                optional boolean a-boolean;
                optional double a-double;
                optional binary an-id;
                optional int64 a-long;
                optional binary a-string;
            }
            """);
    readers = Map.of(
            "a-boolean", mock(BooleanReader.class),
            "a-double", mock(DoubleReader.class),
            "an-id", mock(IdReader.class),
            "a-long", mock(LongReader.class),
            "a-string", mock(StringReader.class)
    );
    readers.values().forEach(reader -> when(reader.asPrimitiveConverter()).thenReturn(reader));
    toTest = new RecordMaterializerImpl(schema, readers);
  }

  @Test
  void readersAreResetIfCurrentRecordIsSkipped() {
    toTest.skipCurrentRecord();
    for (var reader : readers.values()) {
      verify(reader, times(1)).reset();
    }
  }

  @Test
  void fillsReaders() {
    var root = toTest.getRootConverter();
    var booleanConverter = root.getConverter(0).asPrimitiveConverter();
    var doubleConverter = root.getConverter(1).asPrimitiveConverter();
    var idConverter = root.getConverter(2).asPrimitiveConverter();
    var longConverter = root.getConverter(3).asPrimitiveConverter();
    var stringConverter = root.getConverter(4).asPrimitiveConverter();

    booleanConverter.addBoolean(true);
    doubleConverter.addDouble(3.14);
    idConverter.addBinary(Binary.fromConstantByteArray(new byte[] { 1, 2, 3, 4 }));
    longConverter.addLong(73L);
    stringConverter.addBinary(Binary.fromString("The quick brown fox jumps over the lazy dog"));

    var booleanReader = readers.get("a-boolean");
    var doubleReader = readers.get("a-double");
    var idReader = readers.get("an-id");
    var longReader = readers.get("a-long");
    var stringReader = readers.get("a-string");

    verify(booleanReader, times(1)).addBoolean(true);
    verify(doubleReader, times(1)).addDouble(3.14);
    verify(idReader, times(1)).addBinary(Binary.fromConstantByteArray(new byte[] { 1, 2, 3, 4 }));
    verify(longReader, times(1)).addLong(73L);
    verify(stringReader, times(1)).addBinary(Binary.fromString("The quick brown fox jumps over the lazy dog"));
  }

  @Test
  void canGetRootConverter() {
    var rootConverter = toTest.getRootConverter();
    assertThat(rootConverter).isNotNull();
  }
}