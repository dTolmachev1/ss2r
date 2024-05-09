package io.github.dtolmachev1.data.column;

import jakarta.annotation.Nonnull;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class GenericColumn<E> implements Column<E> {
    private String name;
    private ColumnType type;
    private final Map<Integer, E> data;

    public GenericColumn(String name) {
        this(name, ColumnTypeFactory.getColumnType(StringType.TYPE_NAME));
    }

    public GenericColumn(String name, ColumnType type) {
        this.name = name;
        this.type = type;
        this.data = new HashMap<>();
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
    public ColumnType getType() {
        return this.type;
    }

    @Override
    public void setType(ColumnType type) {
        this.type = type;
    }

    @Override
    public void updateType() {
        if (this.type.name().equals(StringType.TYPE_NAME)) {
            ((StringType) this.type).setMaxLength(this.data.values().stream().map(value -> ((String) value).length()).max(Integer::compare).orElse(0));
        }
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
    public boolean contains(Integer id) {
        return this.data.containsKey(id);
    }

    @Override
    public E get(Integer id) {
        return this.data.get(id);
    }

    @Override
    public Integer add(E value) {
        this.data.put(this.data.size(), value);
        return this.data.size() - 1;
    }

    @Override
    public boolean add(Integer id, E value) {
        return Objects.isNull(this.data.putIfAbsent(id, value));
    }

    @Override
    public boolean set(Integer id, E value) {
        return Objects.nonNull(this.data.replace(id, value));
    }

    @Override
    public E remove(Integer id) {
        return this.data.remove(id);
    }

    @Override
    public Stream<Map.Entry<Integer, E>> stream() {
        return this.data.entrySet().stream();
    }

    @Override
    public Stream<Map.Entry<Integer, E>> parallelStream() {
        return this.data.entrySet().parallelStream();
    }

    @Override
    @Nonnull
    public Iterator<Map.Entry<Integer, E>> iterator() {
        return this.data.entrySet().iterator();
    }

    @Override
    public void forEach(Consumer<? super Map.Entry<Integer, E>> action) {
        this.data.entrySet().forEach(action);
    }

    @Override
    public Spliterator<Map.Entry<Integer, E>> spliterator() {
        return this.data.entrySet().spliterator();
    }
}
