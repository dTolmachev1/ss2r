package io.github.dtolmachev1.data.column;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

public class ColumnTypeFactory {
    private static final Map<String, Supplier<ColumnType>> COLUMN_TYPES = Map.ofEntries(
            new AbstractMap.SimpleImmutableEntry<>(StringType.TYPE_NAME, StringType::new),
            new AbstractMap.SimpleImmutableEntry<>(IntegerType.TYPE_NAME, IntegerType::newInstance),
            new AbstractMap.SimpleImmutableEntry<>(DoubleType.TYPE_NAME, DoubleType::newInstance)
    );

    public static ColumnType getColumnType(String name) {
        return Objects.requireNonNullElse(COLUMN_TYPES.get(name), () -> null).get();
    }
}
