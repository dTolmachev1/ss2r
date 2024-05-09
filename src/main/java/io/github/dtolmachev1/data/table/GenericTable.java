package io.github.dtolmachev1.data.table;

import io.github.dtolmachev1.data.column.Column;
import jakarta.annotation.Nonnull;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class GenericTable implements Table {
    private String name;
    private final List<Constraint> constraints;
    private final List<Path> sources;
    private final Map<String, Column<?>> data;

    public GenericTable(String name) {
        this.name = name;
        this.data = new LinkedHashMap<>();
        this.constraints = new ArrayList<>();
        this.sources = new ArrayList<>();
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public void addConstraint(Constraint constraint) {
        this.constraints.add(constraint);
    }

    @Override
    public List<Constraint> constraints() {
        return List.copyOf(this.constraints);
    }

    @Override
    public Optional<Column<?>> columnWithUniqueConstraint() {
        return this.constraints.stream()
                .filter(constraint -> constraint.name().equals(UniqueConstraint.CONSTRAINT_NAME))
                .findAny()
                .map(constraint -> ((UniqueConstraint) constraint).column());
    }

    @Override
    public void addSource(Path source) {
        this.sources.add(source);
    }

    @Override
    public List<Path> sources() {
        return List.copyOf(this.sources);
    }

    @Override
    public boolean isEmpty() {
        return this.data.isEmpty();
    }

    @Override
    public int size() {
        return this.data.size();
    }

    @Override
    public void clear() {
        this.data.clear();
    }

    @Override
    public boolean contains(String columnName) {
        return this.data.containsKey(columnName);
    }

    @Override
    public Column<?> get(String columnName) {
        return this.data.get(columnName);
    }

    @Override
    public boolean add(Column<?> column) {
        return Objects.isNull(this.data.putIfAbsent(column.getName(), column));
    }

    @Override
    public boolean set(Column<?> column) {
        return Objects.nonNull(this.data.replace(column.getName(), column));
    }

    @Override
    public Column<?> remove(String columnName) {
        return this.data.remove(columnName);
    }

    @Override
    public Stream<Map.Entry<String, Column<?>>> stream() {
        return this.data.entrySet().stream();
    }

    @Override
    public Stream<Map.Entry<String, Column<?>>> parallelStream() {
        return this.data.entrySet().parallelStream();
    }

    @Override
    @Nonnull
    public Iterator<Map.Entry<String, Column<?>>> iterator() {
        return this.data.entrySet().iterator();
    }

    @Override
    public void forEach(Consumer<? super Map.Entry<String, Column<?>>> action) {
        this.data.entrySet().forEach(action);
    }

    @Override
    public Spliterator<Map.Entry<String, Column<?>>> spliterator() {
        return this.data.entrySet().spliterator();
    }
}
