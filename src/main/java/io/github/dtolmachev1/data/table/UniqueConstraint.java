package io.github.dtolmachev1.data.table;

import io.github.dtolmachev1.data.column.Column;

public class UniqueConstraint implements Constraint {
    public static final String CONSTRAINT_NAME = "unique";
    private Column<?> column;

    private UniqueConstraint() {
    }

    public UniqueConstraint(Column<?> column) {
        this.column = column;
    }

    @Override
    public String name() {
        return CONSTRAINT_NAME;
    }

    public Column<?> column() {
        return this.column;
    }

    public static UniqueConstraintBuilder builder() {
        return new UniqueConstraintBuilder();
    }

    public static class UniqueConstraintBuilder implements ConstraintBuilder {
        private final UniqueConstraint uniqueConstraint;

        private UniqueConstraintBuilder() {
            this.uniqueConstraint = new UniqueConstraint();
        }

        public UniqueConstraintBuilder column(Column<?> column) {
            this.uniqueConstraint.column = column;
            return this;
        }

        @Override
        public UniqueConstraint build() {
            return this.uniqueConstraint;
        }
    }
}
