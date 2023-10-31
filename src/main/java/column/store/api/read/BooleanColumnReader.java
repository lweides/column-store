package column.store.api.read;

public interface BooleanColumnReader extends ColumnReader {

    /**
     * Read the value of the {@link column.store.api.column.BooleanColumn} of the current record.
     *
     * <p> This method may block, until the value for the current record has been read.
     *
     * @return the value of this {@link column.store.api.column.BooleanColumn} of the current record.
     * @throws NullPointerException if the value is {@code null}.
     */
    boolean get();
}
