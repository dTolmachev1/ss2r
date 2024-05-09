package io.github.dtolmachev1.data.column;

import java.util.Map;
import java.util.stream.Stream;

public interface Column<E> extends Iterable<Map.Entry<Integer, E>> {
    String getName();

    void setName(String name);

    ColumnType getType();

    void setType(ColumnType type);

    void updateType();

    boolean isEmpty();

    int size();

    void clear();

    boolean contains(Integer id);

    E get(Integer id);

    Integer add(E value);

    boolean add(Integer id, E value);

    boolean set(Integer id, E value);

    E remove(Integer id);

    Stream<Map.Entry<Integer, E>> stream();

    Stream<Map.Entry<Integer, E>> parallelStream();
}
