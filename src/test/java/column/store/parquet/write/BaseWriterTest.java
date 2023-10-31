package column.store.parquet.write;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.apache.parquet.io.api.RecordConsumer;
import org.junit.jupiter.api.Test;

abstract class BaseWriterTest<W extends BaseWriter, V> {

  /** The implementation of {@link BaseWriter} to be tested. */
  protected abstract W writer();

  /** Write a value to the current {@code writer} and return it */
  protected abstract V write(W writer);

  /** Verifies that {@code value} was written to the {@code writer} */
  protected abstract void verifyValueWritten(V value, RecordConsumer recordConsumer);

  @Test
  void writerHasNoValueInitially() {
    var writer = writer();

    assertThat(writer.isPresent)
            .as("Writer should not contain a value when nothing is written")
            .isFalse();
  }

  @Test
  void writingNullIsNotPresent() {
    var writer = writer();
    writer.writeNull();

    assertThat(writer.isPresent)
            .as("Writer should not contain a value when null is written")
            .isFalse();
  }

  @Test
  void writesStartAndEndFieldWhenNotPresent() {
    var writer = writer();
    var recordConsumer = mock(RecordConsumer.class);
    assertThat(writer.isPresent).isFalse();

    writer.accept(recordConsumer);

    assertThat(writer.isPresent).isFalse();

    verify(recordConsumer, times(1)).startField(any(), anyInt());
    verify(recordConsumer, times(1)).endField(any(), anyInt());
    verifyNoMoreInteractions(recordConsumer);
  }

  @Test
  void writesStartEndAndValueWhenPresent() {
    var writer = writer();
    var recordConsumer = mock(RecordConsumer.class);
    var written = write(writer);
    assertThat(writer.isPresent).isTrue();

    writer.accept(recordConsumer);

    assertThat(writer.isPresent).isFalse();

    verify(recordConsumer, times(1)).startField(any(), anyInt());
    verifyValueWritten(written, recordConsumer);
    verify(recordConsumer, times(1)).endField(any(), anyInt());
  }
}