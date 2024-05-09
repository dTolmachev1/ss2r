package io.github.dtolmachev1.inference.policy;

import io.github.dtolmachev1.data.table.Table;
import io.github.dtolmachev1.inference.validator.ColumnValidator;

public interface ColumnPolicy {
    String name();

    void apply(Table table, ColumnValidator columnValidator);
}
