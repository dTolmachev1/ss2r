package io.github.dtolmachev1.ss2r.data.table;

import io.github.dtolmachev1.ss2r.configuration.Configuration;
import io.github.dtolmachev1.ss2r.configuration.XmlConfiguration;
import io.github.dtolmachev1.ss2r.data.column.Column;
import io.github.dtolmachev1.ss2r.data.constraint.Constraint;
import io.github.dtolmachev1.ss2r.data.column.ColumnType;
import io.github.dtolmachev1.ss2r.data.column.SimpleColumn;
import io.github.dtolmachev1.ss2r.data.constraint.UniqueConstraint;
import jakarta.annotation.Nonnull;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SimpleTable implements Table {
    private String name;
    private final Map<String, Column<?>> table;
    private final List<Constraint> constraints;
    private final Configuration configuration;

    public SimpleTable() {
        this("");
    }

    public SimpleTable(String name) {
        this.name = name;
        this.table = new LinkedHashMap<>();
        this.constraints = new ArrayList<>();
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
        return this.table.isEmpty();
    }

    @Override
    public int size() {
        return this.table.size();
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
                .map(Constraint::column);
    }

    @Override
    public boolean contains(String columnName) {
        return this.table.containsKey(columnName);
    }

    @Override
    public Column<?> get(String columnName) throws NoSuchElementException {
        if (!this.table.containsKey(columnName)) {
            throw new NoSuchElementException("Column not found");
        }
        return this.table.get(columnName);
    }

    @Override
    public void add(Column<?> column) throws IllegalArgumentException {
        if (this.table.containsKey(column.name())) {
            throw new IllegalArgumentException("Column with such name already exists");
        }
        this.table.put(column.name(), column);
    }

    @Override
    public void set(Column<?> column) throws NoSuchElementException {
        if (!this.table.containsKey(column.name())) {
            throw new NoSuchElementException("Column not found");
        }
        this.table.replace(column.name(), column);
    }

    @Override
    public void remove(String columnName) throws NoSuchElementException {
        if (!this.table.containsKey(columnName)) {
            throw new NoSuchElementException("Column not found");
        }
        this.table.remove(columnName);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void normalize() {
        stream()
                .filter(entry -> entry.getValue().type().equals(ColumnType.STRING))
                .forEach(entry -> entry.setValue(inferType((Column<String>) entry.getValue())));
        determineUnique();
    }

    @Override
    public Stream<Entry<String, Column<?>>> stream() {
        return this.table.entrySet().stream();
    }

    @Override
    public Stream<Entry<String, Column<?>>> parallelStream() {
        return this.table.entrySet().parallelStream();
    }

    @Override
    @Nonnull
    public Iterator<Entry<String, Column<?>>> iterator() {
        return this.table.entrySet().iterator();
    }

    @Override
    public void forEach(Consumer<? super Entry<String, Column<?>>> action) {
        this.table.entrySet().forEach(action);
    }

    @Override
    public Spliterator<Entry<String, Column<?>>> spliterator() {
        return this.table.entrySet().spliterator();
    }

    private Column<?> inferType(Column<String> column) {
        Column<Integer> integerColumn = new SimpleColumn<>(column.name(), ColumnType.INTEGER);
        Column<Double> doubleColumn = new SimpleColumn<>(column.name(), ColumnType.DOUBLE);
        Map<Integer, String> invalidIntegerValues = new HashMap<>();
        Map<Integer, String> invalidDoubleValues = new HashMap<>();
        for (Entry<Integer, String> entry : column) {
            toInteger(entry.getKey(), entry.getValue(), integerColumn, invalidIntegerValues);
            toDouble(entry.getKey(), entry.getValue(), doubleColumn, invalidDoubleValues);
        }
        if ((double) integerColumn.size() / column.size() >= this.configuration.typeThreshold() && integerColumn.size() >= doubleColumn.size()) {
            resolveType(integerColumn, invalidIntegerValues);
            return integerColumn;
        } else if ((double) doubleColumn.size() / column.size() >= this.configuration.typeThreshold()) {
            resolveType(doubleColumn, invalidDoubleValues);
            return doubleColumn;
        }
        return column;
    }

    @SuppressWarnings({"OptionalIsPresent", "unchecked"})
    private void determineUnique() {
        Map<String, Map<String, List<Integer>>> invalidStringValues = stream()
                .filter(entry -> entry.getValue().type().equals(ColumnType.STRING))
                .map(entry -> new SimpleImmutableEntry<>(entry.getKey(), findMultipleOccurrences((Column<String>) entry.getValue())))
                .filter(entry -> 1 - ((double) totalSize(entry.getValue()) / this.table.get(entry.getKey()).size()) >= this.configuration.uniqueThreshold())
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        Map<String, Map<Integer, List<Integer>>> invalidIntegerValues = stream()
                .filter(entry -> entry.getValue().type().equals(ColumnType.INTEGER))
                .map(entry -> new SimpleImmutableEntry<>(entry.getKey(), findMultipleOccurrences((Column<Integer>) entry.getValue())))
                .filter(entry -> 1 - ((double) totalSize(entry.getValue()) / this.table.get(entry.getKey()).size()) >= this.configuration.uniqueThreshold())
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        Map<String, Map<Double, List<Integer>>> invalidDoubleValues = stream()
                .filter(entry -> entry.getValue().type().equals(ColumnType.DOUBLE))
                .map(entry -> new SimpleImmutableEntry<>(entry.getKey(), findMultipleOccurrences((Column<Double>) entry.getValue())))
                .filter(entry -> 1 - ((double) totalSize(entry.getValue()) / this.table.get(entry.getKey()).size()) >= this.configuration.uniqueThreshold())
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        Optional<String> stringCandidate = determineCandidateColumn(invalidStringValues);
        Optional<String> integerCandidate = determineCandidateColumn(invalidIntegerValues);
        Optional<String> doubleCandidate = determineCandidateColumn(invalidDoubleValues);
        if (integerCandidate.isPresent() && totalSize(invalidIntegerValues.get(integerCandidate.get())) == Collections.min(List.of(stringCandidate.map(candidate -> totalSize(invalidStringValues.get(candidate))).orElse(Integer.MAX_VALUE), totalSize(invalidIntegerValues.get(integerCandidate.get())), doubleCandidate.map(candidate -> totalSize(invalidDoubleValues.get(candidate))).orElse(Integer.MAX_VALUE)))) {
            resolveUnique((Column<Integer>) this.table.get(integerCandidate.get()), invalidIntegerValues.get(integerCandidate.get()));
        } else if (stringCandidate.isPresent() && totalSize(invalidStringValues.get(stringCandidate.get())) <= doubleCandidate.map(candidate -> totalSize(invalidDoubleValues.get(candidate))).orElse(Integer.MAX_VALUE)) {
            resolveUnique((Column<String>) this.table.get(stringCandidate.get()), invalidStringValues.get(stringCandidate.get()));
        } else if (doubleCandidate.isPresent()) {
            resolveUnique((Column<Double>) this.table.get(doubleCandidate.get()), invalidDoubleValues.get(doubleCandidate.get()));
        }
    }

    private void toInteger(Integer id, String value, Column<Integer> integerColumn, Map<Integer, String> invalidValues) {
        try {
            integerColumn.add(id, Integer.valueOf(value));
        } catch (NumberFormatException e) {
            invalidValues.put(id, value);
        }
    }

    private void toDouble(Integer id, String value, Column<Double> doubleColumn, Map<Integer, String> invalidValues) {
        try {
            doubleColumn.add(id, Double.valueOf(value));
        } catch (NumberFormatException e) {
            invalidValues.put(id, value);
        }
    }

    private <E> void resolveType(Column<E> column, Map<Integer, String> invalidValues) {
        Map<Integer, E> resolvedValues = this.configuration.typePolicy().resolve(invalidValues);
        resolvedValues.forEach(column::add);
        Map<Integer, String> unresolvedValues = invalidValues.entrySet().stream()
                .filter(entry -> !resolvedValues.containsKey(entry.getKey()))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        stream()
                .filter(entry -> !entry.getKey().equals(column.name()))
                .forEach(entry -> unresolvedValues.forEach((key, value) -> entry.getValue().remove(key)));
    }

    private <E> Map<E, List<Integer>> findMultipleOccurrences(Column<E> column) {
        Map<E, List<Integer>> multipleOccurrences = column.stream().collect(Collectors.groupingBy(Entry::getValue, Collectors.mapping(Entry::getKey, Collectors.toList())));
        multipleOccurrences.entrySet().removeIf(entry -> entry.getValue().size() == 1);
        return multipleOccurrences;
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

    private <E> void resolveUnique(Column<E> column, Map<E, List<Integer>> invalidValues) {
        Map<Integer, E> resolvedValues = this.configuration.uniquePolicy().resolve(invalidValues);
        Map<Integer, E> unresolvedValues = invalidValues.entrySet().stream()
                .flatMap(entry -> entry.getValue().stream().map(id -> new SimpleImmutableEntry<>(id, entry.getKey())))
                .filter(entry -> !resolvedValues.containsKey(entry.getKey()))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        this.table.values().forEach(anotherColumn -> unresolvedValues.forEach((key, value) -> anotherColumn.remove(key)));
        resolvedValues.forEach(column::set);
        addConstraint(new UniqueConstraint(column));
    }

    private <E> int totalSize(Map<E, List<Integer>> multimap) {
        return multimap.values().stream().mapToInt(List::size).sum();
    }
}
