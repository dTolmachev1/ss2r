package io.github.dtolmachev1.inference.validator;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

public class TableValidatorBuilderFactory {
    private static final Map<String, Supplier<TableValidator.TableValidatorBuilder>> TABLE_VALIDATOR_BUILDERS = Map.ofEntries(
            new AbstractMap.SimpleImmutableEntry<>(SimilarTablesValidator.VALIDATOR_NAME, SimilarTablesValidator::builder)
    );

    public static TableValidator.TableValidatorBuilder getTableValidatorBuilder(String name) {
        return Objects.requireNonNullElse(TABLE_VALIDATOR_BUILDERS.get(name), () -> null).get();
    }
}
