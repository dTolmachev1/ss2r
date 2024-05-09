package io.github.dtolmachev1.inference.policy;

import io.github.dtolmachev1.data.table.Table;
import io.github.dtolmachev1.inference.validator.TableValidator;

public interface TablePolicy {
    String name();

    void apply(Table table, TableValidator tableValidator);
}
