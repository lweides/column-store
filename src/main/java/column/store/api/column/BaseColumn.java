package column.store.api.column;

import static column.store.util.Conditions.checkArgument;

import java.util.Objects;

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

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BaseColumn that = (BaseColumn) o;
        return Objects.equals(name, that.name) && type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type);
    }
}
