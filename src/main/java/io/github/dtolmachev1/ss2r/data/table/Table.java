package io.github.dtolmachev1.ss2r.data.table;

import io.github.dtolmachev1.ss2r.data.column.Column;
import io.github.dtolmachev1.ss2r.data.constraint.Constraint;

import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Stream;

public interface Table extends Iterable<Entry<String, Column<?>>> {
    String name();

    void setName(String name);

    boolean isEmpty();

    int size();

    void addConstraint(Constraint constraint);

    List<Constraint> constraints();

    Optional<Column<?>> columnWithUniqueConstraint();

    boolean contains(String columnName);

    Column<?> get(String columnName) throws NoSuchElementException;

    void add(Column<?> column) throws IllegalArgumentException;

    void set(Column<?> column) throws NoSuchElementException;

    void remove(String columnName) throws NoSuchElementException;

    void normalize();

    Stream<Entry<String, Column<?>>> stream();

    Stream<Entry<String, Column<?>>> parallelStream();
}
