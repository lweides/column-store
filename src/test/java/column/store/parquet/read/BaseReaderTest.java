package column.store.parquet.read;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.NoSuchElementException;

import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.Test;

abstract class BaseReaderTest<R extends BaseReader, V> {

  /** The implementation of {@link BaseReader} to be tested. */
  protected abstract R reader();

  /** Add a value to the current {@code reader} and return it */
  protected abstract V add(R reader);

  /** Retrieve the current value from the {@code reader}. */
  protected abstract V get(R reader);

  /** Number of unsupported operations. Can be less than 4 in case of readers interacting with {@link Binary}*/
  protected int unsupportedOperations() {
    return 4;
  }

  @Test
  void readerHasNoValueInitially() {
    var reader = reader();

    assertThat(reader.isPresent()).isFalse();
    assertThat(reader.isNull()).isTrue();
  }

  @Test
  void addedValueCanBeRetrieved() {
    var reader = reader();
    var added = add(reader);

    assertThat(reader.isPresent()).isTrue();
    assertThat(reader.isNull()).isFalse();
    assertThat(get(reader)).isEqualTo(added);
    assertThat(get(reader))
            .as("Value can be retrieved multiple times")
            .isEqualTo(added);
  }

  @Test
  void readerCanBeReset() {
    var reader = reader();
    add(reader);
    reader.reset();

    assertThat(reader.isPresent())
            .as("Reader should not contain a value")
            .isFalse();

    assertThat(reader.isNull())
            .as("Reader should not contain a value")
            .isTrue();
  }

  @Test
  void readingNonPresentValueThrows() {
    var reader = reader();

    assertThatThrownBy(() -> get(reader))
            .isInstanceOf(NoSuchElementException.class)
            .hasMessage("Current value is null");
  }

  @Test
  void readerSupportsOnlyOneType() {
    var reader = reader();
    int unsupportedOperations = 0;

    List<Runnable> operations = List.of(
            () -> reader.addBoolean(true),
            () -> reader.addDouble(3.14),
            () -> reader.addBinary(Binary.fromConstantByteArray(new byte[] { 1, 2, 3, 4 })),
            () -> reader.addLong(73L),
            () -> reader.addBinary(Binary.fromString("The quick brown fox jumps over the lazy dog"))
    );

    for (var operation : operations) {
      try {
        operation.run();
      } catch (Exception e) {
        unsupportedOperations++;
      }
    }

    assertThat(unsupportedOperations).isEqualTo(unsupportedOperations());
  }
}