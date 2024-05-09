package io.github.dtolmachev1.data.table;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

public class ConstraintBuilderFactory {
    private static final Map<String, Supplier<Constraint.ConstraintBuilder>> CONSTRAINT_BUILDERS = Map.ofEntries(
            new AbstractMap.SimpleImmutableEntry<>(UniqueConstraint.CONSTRAINT_NAME, UniqueConstraint::builder),
            new AbstractMap.SimpleImmutableEntry<>(ReferenceConstraint.CONSTRAINT_NAME, ReferenceConstraint::builder)
    );

    public static Constraint.ConstraintBuilder getConstraintBuilder(String name) {
        return Objects.requireNonNullElse(CONSTRAINT_BUILDERS.get(name), () -> null).get();
    }
}
