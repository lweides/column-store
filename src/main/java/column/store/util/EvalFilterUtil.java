package column.store.util;

import column.store.api.query.*;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public final class EvalFilterUtil {

    private static final Base64.Encoder BASE_64 = Base64.getEncoder();

    private EvalFilterUtil() { }

    public static boolean eval(final String columnValue, final Filter filter) {
        switch (filter.column().type()) {

            case BOOLEAN -> {
                if ("true".equalsIgnoreCase(columnValue) || "false".equalsIgnoreCase(columnValue)) {
                    return evalBooleanFilter(Boolean.parseBoolean(columnValue), (BooleanFilter) filter);
                } else {
                    return false;
                }
            }
            case DOUBLE -> {
                try {
                    return evalDoubleFilter(Double.parseDouble(columnValue), (DoubleFilter) filter);
                } catch (NumberFormatException e) {
                    return false;
                }
            }
            case ID -> {
                return evalIdFilter(columnValue, (IdFilter) filter);
            }
            case LONG -> {
                try {
                    return evalLongFilter(Long.parseLong(columnValue), (LongFilter) filter);
                } catch (NumberFormatException e) {
                    return false;
                }
            }
            case STRING -> {
                return evalStringFilter(columnValue, (StringFilter) filter);
            }

            default -> {
                return false;
            }
        }
    }

    private static boolean evalBooleanFilter(final boolean columnValue, final BooleanFilter filter) {
        return filter.value() == columnValue;
    }

    private static boolean evalDoubleFilter(final double columnValue, final DoubleFilter filter) {
        switch (filter.matchType()) {
            case LESS_THAN -> {
                return columnValue < filter.upperBound();
            }
            case GREATER_THAN -> {
                return columnValue > filter.lowerBound();
            }
            case BETWEEN -> {
                return columnValue >= filter.lowerBound() && columnValue < filter.upperBound();
            }

            default -> {
                return false;
            }
        }
    }

    private static boolean evalIdFilter(final String columnValue, final IdFilter filter) {
        return new String(BASE_64.encode(filter.id()), StandardCharsets.UTF_8).equals(columnValue);
    }

    private static boolean evalLongFilter(final long columnValue, final LongFilter filter) {
        switch (filter.matchType()) {
            case LESS_THAN -> {
                return columnValue < filter.upperBound();
            }
            case GREATER_THAN -> {
                return columnValue > filter.lowerBound();
            }
            case BETWEEN -> {
                return columnValue >= filter.lowerBound() && columnValue < filter.upperBound();
            }

            default -> {
                return false;
            }
        }
    }

    private static boolean evalStringFilter(final String columnValue, final StringFilter filter) {
        String filterValue = filter.value();
        switch (filter.matchType()) {
            case IS -> {
                return columnValue.equals(filterValue);
            }
            case STARTS_WITH -> {
                return columnValue.startsWith(filterValue);
            }
            case ENDS_WITH -> {
                return columnValue.endsWith(filterValue);
            }
            case CONTAINS -> {
                return columnValue.contains(filterValue);
            }

            default -> {
                return false;
            }
        }
    }
}
