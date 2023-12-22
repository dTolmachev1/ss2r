package io.github.dtolmachev1.ss2r.data.database;

import io.github.dtolmachev1.ss2r.data.table.Table;

import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

public interface Database extends Iterable<Entry<String, Table>> {
    String name();

    void setName(String name);

    boolean isEmpty();

    int size();

    Table get(String tableName) throws NoSuchElementException;

    void add(Table table) throws IllegalArgumentException;

    void set(Table table) throws NoSuchElementException;

    void remove(String tableName) throws NoSuchElementException;

    void normalize();

    Stream<Entry<String, Table>> stream();

    Stream<Entry<String, Table>> parallelStream();
}
