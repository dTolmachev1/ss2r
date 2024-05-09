package io.github.dtolmachev1.inference.policy;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

public class TablePolicyFactory {
    private static final Map<String, Supplier<TablePolicy>> TABLE_POLICIES = Map.ofEntries(
            new AbstractMap.SimpleImmutableEntry<>(RemovePolicy.POLICY_NAME, RemovePolicy::newInstance)
    );

    public static TablePolicy getTablePolicy(String name) {
        return Objects.requireNonNullElse(TABLE_POLICIES.get(name), () -> null).get();
    }
}
