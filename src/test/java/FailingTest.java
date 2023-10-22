import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FailingTest {

    @Test
    void shouldFail() {
        assertThat(1).isZero();
    }
}
