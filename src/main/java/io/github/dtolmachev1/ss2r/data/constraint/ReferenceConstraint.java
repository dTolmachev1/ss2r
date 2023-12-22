package io.github.dtolmachev1.ss2r.data.constraint;

import io.github.dtolmachev1.ss2r.data.column.Column;
import io.github.dtolmachev1.ss2r.data.table.Table;

import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

public class ReferenceConstraint implements Constraint {
    public static final String CONSTRAINT_NAME = "reference";
    private final Column<?> column;
    private final Table referencedTable;
    private final String referencedColumnName;

    public ReferenceConstraint(Column<?> column, Table referencedTable, String referencedColumnName) {
        this.column = column;
        this.referencedTable = referencedTable;
        this.referencedColumnName = referencedColumnName;
    }

    @Override
    public String name() {
        return CONSTRAINT_NAME;
    }

    @Override
    public Column<?> column() {
        return this.column;
    }

    public Table referencedTable() {
        return this.referencedTable;
    }

    public Column<?> referencedColumn() {
        return this.referencedTable.get(this.referencedColumnName);
    }

    @SuppressWarnings({"DuplicateBranchesInSwitch", "unchecked"})
    @Override
    public boolean validate() {
        if (!this.column.type().equals(this.referencedTable.get(this.referencedColumnName).type())) {
            return false;
        }
        return switch (this.column.type()) {
            case STRING -> validateReference((Column<String>) this.column, (Column<String>) this.referencedTable.get(this.referencedColumnName));
            case INTEGER -> validateReference((Column<Integer>) this.column, (Column<Integer>) this.referencedTable.get(this.referencedColumnName));
            case DOUBLE -> validateReference((Column<Double>) this.column, (Column<Double>) this.referencedTable.get(this.referencedColumnName));
        };
    }

    private <E> boolean validateReference(Column<E> referencing, Column<E> referenced) {
        Set<E> referencingSet = referencing.stream()
                .map(Entry::getValue)
                .collect(Collectors.toSet());
        Set<E> referencedSet = referenced.stream()
                .map(Entry::getValue)
                .collect(Collectors.toSet());
        referencingSet.removeAll(referencedSet);
        return referencingSet.isEmpty();
    }
}
