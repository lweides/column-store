package column.store.api.column;

public final class StringColumn extends BaseColumn {
    StringColumn(final String name) {
        super(name, Type.STRING);
    }
}
