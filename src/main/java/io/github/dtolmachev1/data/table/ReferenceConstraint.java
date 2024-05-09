package io.github.dtolmachev1.data.table;

import io.github.dtolmachev1.data.column.Column;

public class ReferenceConstraint implements Constraint {
    public static final String CONSTRAINT_NAME = "reference";
    private Column<?> referencingColumn;
    private Table referencedTable;
    private Column<?> referencedColumn;

    private ReferenceConstraint() {
    }

    public ReferenceConstraint(Column<?> referencingColumn, Table referencedTable, Column<?> referencedColumn) {
        this.referencingColumn = referencingColumn;
        this.referencedTable = referencedTable;
        this.referencedColumn = referencedColumn;
    }

    @Override
    public String name() {
        return CONSTRAINT_NAME;
    }

    public Column<?> referencingColumn() {
        return this.referencingColumn;
    }

    public Table referencedTable() {
        return this.referencedTable;
    }

    public Column<?> referencedColumn() {
        return this.referencedColumn;
    }

    public static ReferenceConstraintBuilder builder() {
        return new ReferenceConstraintBuilder();
    }

    public static class ReferenceConstraintBuilder implements ConstraintBuilder {
        private final ReferenceConstraint referenceConstraint;

        private ReferenceConstraintBuilder() {
            this.referenceConstraint = new ReferenceConstraint();
        }

        public ReferenceConstraintBuilder referencingColumn(Column<?> referencingColumn) {
            this.referenceConstraint.referencingColumn = referencingColumn;
            return this;
        }

        public ReferenceConstraintBuilder referencedTable(Table referencedTable) {
            this.referenceConstraint.referencedTable = referencedTable;
            return this;
        }

        public ReferenceConstraintBuilder referencedColumn(Column<?> referencedColumn) {
            this.referenceConstraint.referencedColumn = referencedColumn;
            return this;
        }

        @Override
        public ReferenceConstraint build() {
            return this.referenceConstraint;
        }
    }
}
