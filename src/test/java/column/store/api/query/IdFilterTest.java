package column.store.api.query;

import column.store.api.column.Column;
import column.store.api.column.IdColumn;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class IdFilterTest {

    private final IdColumn column = Column.forId("some-column");
    static List<byte[]> bytes() {
        return List.of(
            new byte[] { 1, 2, 3, 4 },
            new byte[] { 73, 42, 127, 0}
        );
    }

    @ParameterizedTest
    @MethodSource("bytes")
    void isFilter(byte[] id) {
        var is = Filter.whereId(column).is(id);
        assertThat(is.id()).isEqualTo(id);
        assertThat(is.column()).isEqualTo(column);
    }
}