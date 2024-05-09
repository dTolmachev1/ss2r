package io.github.dtolmachev1.data.table;

public interface Constraint {
    String name();

    interface ConstraintBuilder {
        Constraint build();
    }
}
