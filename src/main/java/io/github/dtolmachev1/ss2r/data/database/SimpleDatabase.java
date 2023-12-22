package io.github.dtolmachev1.ss2r.data.database;

import io.github.dtolmachev1.ss2r.configuration.Configuration;
import io.github.dtolmachev1.ss2r.configuration.XmlConfiguration;
import io.github.dtolmachev1.ss2r.data.column.Column;
import io.github.dtolmachev1.ss2r.data.constraint.ReferenceConstraint;
import io.github.dtolmachev1.ss2r.data.table.Table;
import jakarta.annotation.Nonnull;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SimpleDatabase implements Database {
    private String name;
    private final Map<String, Table> database;
    private final Configuration configuration;

    public SimpleDatabase() {
        this("");
    }

    public SimpleDatabase(String name) {
        this.name = name;
        this.database = new HashMap<>();
        this.configuration = XmlConfiguration.newInstance();
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
    public boolean isEmpty() {
        return this.database.isEmpty();
    }

    @Override
    public int size() {
        return this.database.size();
    }

    @Override
    public Table get(String tableName) throws NoSuchElementException {
        if (!this.database.containsKey(tableName)) {
            throw new NoSuchElementException("Table not found");
        }
        return this.database.get(tableName);
    }

    @Override
    public void add(Table table) throws IllegalArgumentException {
        if (this.database.containsKey(table.name())) {
            throw new IllegalArgumentException("Table already exists");
        }
        this.database.put(table.name(), table);
    }

    @Override
    public void set(Table table) throws NoSuchElementException {
        if (!this.database.containsKey(table.name())) {
            throw new NoSuchElementException("Table not found");
        }
        this.database.replace(table.name(), table);
    }

    @Override
    public void remove(String tableName) throws NoSuchElementException {
        if (!this.database.containsKey(tableName)) {
            throw new NoSuchElementException("Table not found");
        }
        this.database.remove(tableName);
    }

    @Override
    public void normalize() {
        this.database.values().forEach(Table::normalize);
        Map<String, Column<?>> columnsWithUniqueConstraint = stream()
                .map(entry -> new SimpleImmutableEntry<>(entry.getKey(), entry.getValue().columnWithUniqueConstraint()))
                .filter(entry -> entry.getValue().isPresent())
                .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().get()));
        forEach(tableEntry -> tableEntry.getValue().stream()
                .map(Entry::getValue)
                .filter(column -> !columnsWithUniqueConstraint.containsKey(tableEntry.getKey()) || !columnsWithUniqueConstraint.get(tableEntry.getKey()).name().equals(column.name()))
                .forEach(column -> determineReference(tableEntry.getKey(), column, columnsWithUniqueConstraint.entrySet().stream().filter(columnEntry -> !columnEntry.getKey().equals(tableEntry.getKey()) && columnEntry.getValue().type().equals(column.type())).collect(Collectors.toMap(Entry::getKey, Entry::getValue)))));
    }

    @Override
    public Stream<Entry<String, Table>> stream() {
        return this.database.entrySet().stream();
    }

    @Override
    public Stream<Entry<String, Table>> parallelStream() {
        return this.database.entrySet().parallelStream();
    }

    @Override
    @Nonnull
    public Iterator<Entry<String, Table>> iterator() {
        return this.database.entrySet().iterator();
    }

    @Override
    public void forEach(Consumer<? super Entry<String, Table>> action) {
        this.database.entrySet().forEach(action);
    }

    @Override
    public Spliterator<Entry<String, Table>> spliterator() {
        return this.database.entrySet().spliterator();
    }

    @SuppressWarnings({"DuplicateBranchesInSwitch", "unchecked"})
    private void determineReference(String tableName, Column<?> column, Map<String, Column<?>> candidateColumns) {
        switch (column.type()) {
            case STRING -> determineReferenceColumn(tableName, (Column<String>) column, candidateColumns);
            case INTEGER -> determineReferenceColumn(tableName, (Column<Integer>) column, candidateColumns);
            case DOUBLE -> determineReferenceColumn(tableName, (Column<Double>) column, candidateColumns);
        }
    }

    @SuppressWarnings({"OptionalIsPresent", "unchecked"})
    private <E> void determineReferenceColumn(String tableName, Column<E> column, Map<String, Column<?>> candidateColumns) {
        Map<String, Map<E, List<Integer>>> invalidValues = candidateColumns.entrySet().stream()
                .map(entry -> new SimpleImmutableEntry<>(entry.getKey(), findDanglingReferences(column, (Column<E>) entry.getValue())))
                .filter(entry -> 1 - ((double) totalSize(entry.getValue()) / column.size()) >= this.configuration.referenceThreshold())
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        Optional<String> candidate = determineCandidateColumn(invalidValues);
        if (candidate.isPresent()) {
            resolveReference(this.database.get(tableName), column, this.database.get(candidate.get()), (Column<E>) candidateColumns.get(candidate.get()), invalidValues.get(candidate.get()));
        }
    }

    private <E> Map<E, List<Integer>> findDanglingReferences(Column<E> referencingColumn, Column<E> referencedColumn) {
        Set<E> referencedSet = referencedColumn.stream()
                .map(Entry::getValue)
                .collect(Collectors.toSet());
        return referencingColumn.stream()
                .filter(entry -> !referencedSet.contains(entry.getValue()))
                .collect(Collectors.groupingBy(Entry::getValue, Collectors.mapping(Entry::getKey, Collectors.toList())));
    }

    private <E> Optional<String> determineCandidateColumn(Map<String, Map<E, List<Integer>>> invalidValues) {
        int invalidCount = invalidValues.values().stream()
                .mapToInt(this::totalSize)
                .min()
                .orElse(0);
        return invalidValues.entrySet().stream()
                .filter(entry -> totalSize(entry.getValue()) == invalidCount)
                .findAny()
                .map(Entry::getKey);
    }

    private <E> void resolveReference(Table table, Column<E> column, Table referencedTable, Column<E> referencedColumn, Map<E, List<Integer>> invalidValues) {
        Map<Integer, E> resolvedValues = this.configuration.referencePolicy().resolve(invalidValues);
        Map<Integer, E> unresolvedValues = invalidValues.entrySet().stream()
                .flatMap(entry -> entry.getValue().stream().map(id -> new SimpleImmutableEntry<>(id, entry.getKey())))
                .filter(entry -> !resolvedValues.containsKey(entry.getKey()))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        table.forEach(entry -> unresolvedValues.forEach((key, value) -> entry.getValue().remove(key)));
        resolvedValues.forEach(column::set);
        table.addConstraint(new ReferenceConstraint(column, referencedTable, referencedColumn.name()));
    }

    private <E> int totalSize(Map<E, List<Integer>> multimap) {
        return multimap.values().stream().mapToInt(List::size).sum();
    }
}
