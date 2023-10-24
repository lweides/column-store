package column.store.api.column;

public final class IdColumn extends BaseColumn {
    IdColumn(final String name) {
        super(name, Type.ID);
    }
}
