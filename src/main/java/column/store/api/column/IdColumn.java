package column.store.api.column;

public final class IdColumn extends BaseColumn {
    IdColumn(String name) {
        super(name, Type.ID);
    }
}
