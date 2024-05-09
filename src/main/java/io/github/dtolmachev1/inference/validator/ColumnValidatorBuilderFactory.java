package io.github.dtolmachev1.inference.validator;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

public class ColumnValidatorBuilderFactory {
    private static final Map<String, Supplier<ColumnValidator.ColumnValidatorBuilder>> COLUMN_VALIDATOR_BUILDERS = Map.ofEntries(
            new AbstractMap.SimpleImmutableEntry<>(ColumnTypeValidator.VALIDATOR_NAME, ColumnTypeValidator::builder),
            new AbstractMap.SimpleImmutableEntry<>(UniqueConstraintValidator.VALIDATOR_NAME, UniqueConstraintValidator::builder),
            new AbstractMap.SimpleImmutableEntry<>(ReferenceConstraintValidator.VALIDATOR_NAME, ReferenceConstraintValidator::builder),
            new AbstractMap.SimpleImmutableEntry<>(MultiValueReferenceValidator.VALIDATOR_NAME, MultiValueReferenceValidator::builder)
    );

    public static ColumnValidator.ColumnValidatorBuilder getColumnValidatorBuilder(String name) {
        return Objects.requireNonNullElse(COLUMN_VALIDATOR_BUILDERS.get(name), () -> null).get();
    }
}
