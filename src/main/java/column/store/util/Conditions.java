package column.store.util;

public final class Conditions {

    private Conditions() {
        // hidden util constructor
    }

    public static void checkState(final boolean state, final String msg) {
        if (!state) {
            throw new IllegalStateException(msg);
        }
    }

    public static void checkArgument(final boolean argument, final String msg) {
        if (!argument) {
            throw new IllegalArgumentException(msg);
        }
    }

    public static void checkSupported(final boolean supported, final String msg) {
        if (!supported) {
            throw new UnsupportedOperationException(msg);
        }
    }
}
