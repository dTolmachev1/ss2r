package io.github.dtolmachev1.ss2r.data.column;

import jakarta.annotation.Nonnull;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class SimpleColumn<E> implements Column<E> {
    private String name;
    private final ColumnType type;
    private final Map<Integer, E> column;
    private int maxLength;

    public SimpleColumn() {
        this("", ColumnType.STRING);
    }

    public SimpleColumn(String name) {
        this(name, ColumnType.STRING);
    }

    public SimpleColumn(ColumnType type) {
        this("", type);
    }

    public SimpleColumn(String name, ColumnType type) {
        this.name = name;
        this.type = type;
        this.column = new HashMap<>();
        this.maxLength = 1;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public ColumnType type() {
        return this.type;
    }

    @Override
    public boolean isEmpty() {
        return this.column.isEmpty();
    }

    @Override
    public int size() {
        return this.column.size();
    }

    @Override
    public int maxLength() {
        return this.maxLength;
    }

    @Override
    public boolean contains(Integer id) {
        return this.column.containsKey(id);
    }

    @Override
    public E get(Integer id) throws NoSuchElementException {
        if (!this.column.containsKey(id)) {
            throw new NoSuchElementException("Value not found");
        }
        return this.column.get(id);
    }

    @Override
    public void add(E value) {
        add(this.column.size(), value);
    }

    @Override
    public void add(Integer id, E value) throws IllegalArgumentException {
        if (this.column.containsKey(id)) {
            throw new IllegalArgumentException("Value already exists");
        }
        this.column.put(id, value);
        if (this.type.equals(ColumnType.STRING) && value.toString().length() > this.maxLength) {
            this.maxLength = value.toString().length();
        }
    }

    @Override
    public void set(Integer id, E value) throws NoSuchElementException {
        if (!this.column.containsKey(id)) {
            throw new NoSuchElementException("Value not found");
        }
        this.column.replace(id, value);
        if (this.type.equals(ColumnType.STRING) && value.toString().length() > this.maxLength) {
            this.maxLength = value.toString().length();
        }
    }

    @Override
    public void remove(Integer id) throws NoSuchElementException {
        if (!this.column.containsKey(id)) {
            throw new NoSuchElementException("Value not found");
        }
        this.column.remove(id);
    }

    @Override
    public Stream<Entry<Integer, E>> stream() {
        return this.column.entrySet().stream();
    }

    @Override
    public Stream<Entry<Integer, E>> parallelStream() {
        return this.column.entrySet().parallelStream();
    }

    @Override
    @Nonnull
    public Iterator<Entry<Integer, E>> iterator() {
        return this.column.entrySet().iterator();
    }

    @Override
    public void forEach(Consumer<? super Entry<Integer, E>> action) {
        this.column.entrySet().forEach(action);
    }

    @Override
    public Spliterator<Entry<Integer, E>> spliterator() {
        return this.column.entrySet().spliterator();
    }
}
