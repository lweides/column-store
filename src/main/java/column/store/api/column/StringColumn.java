package column.store.api.column;

public final class StringColumn extends BaseColumn {
    StringColumn(String name) {
        super(name, Type.STRING);
    }
}
