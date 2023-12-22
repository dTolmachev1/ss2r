package io.github.dtolmachev1.ss2r.data.column;

import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

public interface Column<E> extends Iterable<Entry<Integer, E>> {
    String name();

    void setName(String name);

    ColumnType type();

    boolean isEmpty();

    int size();

    int maxLength();

    boolean contains(Integer id);

    E get(Integer id) throws NoSuchElementException;

    void add(E value);

    void add(Integer id, E value) throws IllegalArgumentException;

    void set(Integer id, E value) throws NoSuchElementException;

    void remove(Integer id) throws NoSuchElementException;

    Stream<Entry<Integer, E>> stream();

    Stream<Entry<Integer, E>> parallelStream();
}
