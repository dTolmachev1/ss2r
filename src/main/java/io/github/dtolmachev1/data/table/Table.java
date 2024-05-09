package io.github.dtolmachev1.data.table;

import io.github.dtolmachev1.data.column.Column;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public interface Table extends Iterable<Map.Entry<String, Column<?>>> {
    String getName();

    void setName(String name);

    void addConstraint(Constraint constraint);

    List<Constraint> constraints();

    Optional<Column<?>> columnWithUniqueConstraint();

    void addSource(Path source);

    List<Path> sources();

    boolean isEmpty();

    int size();

    void clear();

    boolean contains(String columnName);

    Column<?> get(String columnName);

    boolean add(Column<?> column);

    boolean set(Column<?> column);

    Column<?> remove(String columnName);

    Stream<Map.Entry<String, Column<?>>> stream();

    Stream<Map.Entry<String, Column<?>>> parallelStream();
}
