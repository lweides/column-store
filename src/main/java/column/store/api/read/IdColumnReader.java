package column.store.api.read;

public non-sealed interface IdColumnReader extends ColumnReader {

    /**
     * Read the value of the {@link column.store.api.column.IdColumn} of the current record.
     *
     * <p> This method may block, until the value for the current record has been read.
     *
     * @return the value of this {@link column.store.api.column.IdColumn} of the current record.
     * @throws NullPointerException if the value is {@code null}.
     */
    byte[] get();
}
