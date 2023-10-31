package column.store.api.read;

import column.store.api.column.*;
import column.store.api.query.Query;

import java.io.IOException;

public interface Reader extends AutoCloseable {

    /**
     * Initialize this {@link Reader} to deliver records matching the {@link Query}.
     *
     * <p> Calling this method may block until all matching records have been found.
     *
     * <p> Calling this method invalidates any previously issued queries.
     */
    void query(Query query) throws IOException;

    /**
     * @return a {@link BooleanColumnReader}, which reads the values of the given {@link BooleanColumn}.
     * @throws NoSuchColumnException if the {@link BooleanColumn} was not selected ({@link Query#select(Column...)})
     */
    BooleanColumnReader of(BooleanColumn column);

    /**
     * @return a {@link DoubleColumnReader}, which reads the values of the given {@link DoubleColumn}.
     * @throws NoSuchColumnException if the {@link DoubleColumn} was not selected ({@link Query#select(Column...)})
     */
    DoubleColumnReader of(DoubleColumn column);

    /**
     * @return a {@link IdColumnReader}, which reads the values of the given {@link IdColumn}.
     * @throws NoSuchColumnException if the {@link IdColumn} was not selected ({@link Query#select(Column...)})
     */
    IdColumnReader of(IdColumn column);

    /**
     * @return a {@link LongColumnReader}, which reads the values of the given {@link LongColumn}.
     * @throws NoSuchColumnException if the {@link LongColumn} was not selected ({@link Query#select(Column...)})
     */
    LongColumnReader of(LongColumn column);

    /**
     * @return a {@link StringColumnReader}, which reads the values of the given {@link StringColumn}.
     * @throws NoSuchColumnException if the {@link StringColumn} was not selected ({@link Query#select(Column...)})
     */
    StringColumnReader of(StringColumn column);

    /**
     * Whether there exists a next record to be accessed.
     *
     * <p> May block until a next record has been found, or no next record can be found.
     *
     * @return whether there exists a next record to be accessed.
     */
    boolean hasNext() throws IOException;

    /**
     * Advance this {@link Reader} to the next record.
     * Any column reader obtained from this {@link Reader} e.g., a {@link BooleanColumnReader}, will return the value
     * of the next record after this method returns.
     *
     * <p> May block until a next record has been found, or no next record can be found.
     *
     * @throws java.util.NoSuchElementException if no next record can be found.
     */
    void next() throws IOException;

    /**
     * Releases any underlying resources.
     * Column readers may not be accessed after this method has been called.
     */
    @Override
    void close() throws IOException;
}
