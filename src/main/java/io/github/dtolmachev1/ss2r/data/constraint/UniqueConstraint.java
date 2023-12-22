package io.github.dtolmachev1.ss2r.data.constraint;

import io.github.dtolmachev1.ss2r.data.column.Column;

import java.util.Map.Entry;

public class UniqueConstraint implements Constraint {
    public static final String CONSTRAINT_NAME = "unique";
    private final Column<?> column;

    public UniqueConstraint(Column<?> column) {
        this.column = column;
    }

    @Override
    public String name() {
        return CONSTRAINT_NAME;
    }

    @Override
    public Column<?> column() {
        return this.column;
    }

    @Override
    public boolean validate() {
        return this.column.stream().map(Entry::getValue).distinct().count() == column.size();
    }
}
