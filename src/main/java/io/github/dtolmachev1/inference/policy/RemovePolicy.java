package io.github.dtolmachev1.inference.policy;

import io.github.dtolmachev1.data.table.Table;
import io.github.dtolmachev1.inference.validator.TableValidator;

import java.util.List;
import java.util.Map;

public class RemovePolicy implements TablePolicy {
    public static final String POLICY_NAME = "remove";

    private RemovePolicy() {
    }

    public static RemovePolicy newInstance(){
        return RemovePolicyHolder.REMOVE_POLICY;
    }

    @Override
    public String name() {
        return POLICY_NAME;
    }

    @Override
    public void apply(Table table, TableValidator tableValidator) {
        List<String> invalidColumns = table.stream()
                .map(Map.Entry::getKey)
                .filter(columnName -> !tableValidator.isValid(columnName))
                .toList();
                invalidColumns.forEach(table::remove);
    }

    private static class RemovePolicyHolder {
        private static final RemovePolicy REMOVE_POLICY = new RemovePolicy();
    }
}
