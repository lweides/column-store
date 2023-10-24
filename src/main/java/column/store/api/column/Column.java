package column.store.api.column;

import java.util.Locale;

/**
 * A {@link Column} is a named storage for a single, named attribute.
 * Supported column types are:
 * <ul>
 *     <li>{@link BooleanColumn}</li>
 *     <li>{@link DoubleColumn}</li>
 *     <li>{@link IdColumn}</li>
 *     <li>{@link LongColumn}</li>
 *     <li>{@link StringColumn}</li>
 * </ul>
 *
 * A {@link Column} can be constructed using the utility methods in this class.
 */
public sealed interface Column permits BaseColumn {

    /**
     * @return the name of the {@link Column}.
     */
    String name();

    /**
     * @return the type of the {@link Column}.
     */
    Type type();

    /**
     * Available data types.
     */
    enum Type {
        BOOLEAN,
        DOUBLE,
        ID,
        LONG,
        STRING,
    }

    /**
     * @return a new {@link BooleanColumn}.
     */
    static BooleanColumn forBoolean(final String name) {
        return new BooleanColumn(name.toLowerCase(Locale.ROOT));
    }

    /**
     * @return a new {@link DoubleColumn}.
     */
    static DoubleColumn forDouble(final String name) {
        return new DoubleColumn(name.toLowerCase(Locale.ROOT));
    }

    /**
     * @return a new {@link IdColumn}.
     */
    static IdColumn forId(final String name) {
        return new IdColumn(name.toLowerCase(Locale.ROOT));
    }

    /**
     * @return a new {@link LongColumn}.
     */
    static LongColumn forLong(final String name) {
        return new LongColumn(name.toLowerCase(Locale.ROOT));
    }

    /**
     * @return a new {@link StringColumn}.
     */
    static StringColumn forString(final String name) {
        return new StringColumn(name.toLowerCase(Locale.ROOT));
    }
}
