package column.store.api.column;

import static column.store.util.Conditions.checkArgument;

abstract sealed class BaseColumn implements Column permits BooleanColumn, DoubleColumn, IdColumn, LongColumn, StringColumn {

    private final String name;
    private final Type type;

    BaseColumn(final String name, final Type type) {
        checkArgument(!name.isBlank(), "Name must not be blank");
        this.name = name;
        this.type = type;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Type type() {
        return type;
    }
}
