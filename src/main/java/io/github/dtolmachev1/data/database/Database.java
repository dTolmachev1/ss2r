package io.github.dtolmachev1.data.database;

import io.github.dtolmachev1.data.table.Table;

import java.util.Map;
import java.util.stream.Stream;

public interface Database extends Iterable<Map.Entry<String, Table>> {
    String getName();

    void setName(String name);

    boolean isEmpty();

    int size();

    void clear();

    boolean contains(String tableName);

    Table get(String tableName);

    boolean add(Table table);

    boolean set(Table table);

    Table remove(String tableName);

    Stream<Map.Entry<String, Table>> stream();

    Stream<Map.Entry<String, Table>> parallelStream();
}
