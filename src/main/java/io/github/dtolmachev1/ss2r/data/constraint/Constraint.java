package io.github.dtolmachev1.ss2r.data.constraint;

import io.github.dtolmachev1.ss2r.data.column.Column;

public interface Constraint {
    String name();

    Column<?> column();

    boolean validate();
}
