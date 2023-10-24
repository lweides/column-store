package column.store.api.column;

public class NoSuchColumnException extends RuntimeException {

    private final Column column;

    public NoSuchColumnException(final Column column) {
        super("Column " + column.name() + " of type " + column.type() + " not found");
        this.column = column;
    }

    public Column column() {
        return column;
    }
}
