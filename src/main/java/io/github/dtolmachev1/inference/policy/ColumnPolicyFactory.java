package io.github.dtolmachev1.inference.policy;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

public class ColumnPolicyFactory {
    private static final Map<String, Supplier<ColumnPolicy>> COLUMN_POLICIES = Map.ofEntries(
            new AbstractMap.SimpleImmutableEntry<>(IgnorePolicy.POLICY_NAME, IgnorePolicy::newInstance),
            new AbstractMap.SimpleImmutableEntry<>(KeepFirstPolicy.POLICY_NAME, KeepFirstPolicy::newInstance)
    );

    public static ColumnPolicy getColumnPolicy(String name) {
        return Objects.requireNonNullElse(COLUMN_POLICIES.get(name), () -> null).get();
    }
}
