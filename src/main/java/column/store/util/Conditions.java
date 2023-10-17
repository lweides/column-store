package column.store.util;

public class Conditions {

    private Conditions() {
        // hidden util constructor
    }

    public static void checkState(boolean state, String msg) {
        if (!state) {
            throw new IllegalStateException(msg);
        }
    }

    public static void checkArgument(boolean argument, String msg) {
        if (!argument) {
            throw new IllegalArgumentException(msg);
        }
    }

    public static void checkSupported(boolean supported, String msg) {
        if (!supported) {
            throw new UnsupportedOperationException(msg);
        }
    }
}
