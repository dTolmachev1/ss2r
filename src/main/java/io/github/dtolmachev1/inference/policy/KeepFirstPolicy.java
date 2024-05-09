package io.github.dtolmachev1.inference.policy;

import io.github.dtolmachev1.data.table.Table;
import io.github.dtolmachev1.inference.validator.ColumnValidator;

import java.util.List;
import java.util.Map;

public class KeepFirstPolicy implements ColumnPolicy {
    public static final String POLICY_NAME = "keep-first";

    private KeepFirstPolicy() {
    }

    public static KeepFirstPolicy newInstance() {
        return KeepFirstPolicyHolder.KEEP_FIRST_POLICY;
    }

    @Override
    public String name() {
        return POLICY_NAME;
    }

    @Override
    public void apply(Table table, ColumnValidator columnValidator) {
        List<Integer> invalidValues = table.stream()
                .map(Map.Entry::getValue)
                .findAny()
                .map(entry -> entry.stream()
                        .map(Map.Entry::getKey)
                        .filter(id -> !columnValidator.isValid(id))
                        .skip(1)
                        .toList())
                .orElse(List.of());
        table.stream()
                .map(Map.Entry::getValue)
                .forEach(column -> invalidValues.forEach(column::remove));
    }

    private static class KeepFirstPolicyHolder {
        private static final KeepFirstPolicy KEEP_FIRST_POLICY = new KeepFirstPolicy();
    }
}
