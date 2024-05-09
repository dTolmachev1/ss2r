package io.github.dtolmachev1.data.database;

import io.github.dtolmachev1.data.table.Table;
import jakarta.annotation.Nonnull;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class GenericDatabase implements Database {
    private String name;
    private final Map<String, Table> data;

    public GenericDatabase(String name) {
        this.name = name;
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
    public boolean contains(String tableName) {
        return this.data.containsKey(tableName);
    }

    @Override
    public Table get(String tableName) {
        return this.data.get(tableName);
    }

    @Override
    public boolean add(Table table) {
        return Objects.isNull(this.data.putIfAbsent(table.getName(), table));
    }

    @Override
    public boolean set(Table table) {
        return Objects.nonNull(this.data.replace(table.getName(), table));
    }

    @Override
    public Table remove(String tableName) {
        return this.data.remove(tableName);
    }

    @Override
    public Stream<Map.Entry<String, Table>> stream() {
        return this.data.entrySet().stream();
    }

    @Override
    public Stream<Map.Entry<String, Table>> parallelStream() {
        return this.data.entrySet().parallelStream();
    }

    @Override
    @Nonnull
    public Iterator<Map.Entry<String, Table>> iterator() {
        return this.data.entrySet().iterator();
    }

    @Override
    public void forEach(Consumer<? super Map.Entry<String, Table>> action) {
        this.data.entrySet().forEach(action);
    }

    @Override
    public Spliterator<Map.Entry<String, Table>> spliterator() {
        return this.data.entrySet().spliterator();
    }
}
