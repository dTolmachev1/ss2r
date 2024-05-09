package io.github.dtolmachev1.repository.source;

import com.opencsv.CSVReader;
import io.github.dtolmachev1.data.column.Column;
import io.github.dtolmachev1.data.column.GenericColumn;
import io.github.dtolmachev1.data.database.Database;
import io.github.dtolmachev1.data.database.GenericDatabase;
import io.github.dtolmachev1.data.table.GenericTable;
import io.github.dtolmachev1.data.table.Table;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

public class CsvRepository implements SourceRepository {
    public static final String REPOSITORY_NAME = "csv";
    private static final String SOURCE_EXTENSION = ".csv";
    private static final String TABLE_PREFIX = "table_";
    private static final String COLUMN_PREFIX = "column_";
    private int tableCount;

    private CsvRepository() {
    }

    public static CsvRepository newInstance() {
        return CsvRepositoryHolder.CSV_REPOSITORY;
    }

    @Override
    public Database load(Path path, String databaseName) {
        this.tableCount = 0;
        Database database = new GenericDatabase(databaseName);
        loadCsv(path, database);
        return database;
    }

    private void loadCsv(Path directory, Database database) {
        if (Files.isDirectory(directory)) {
            try (DirectoryStream<Path> directories = Files.newDirectoryStream(directory)) {
                directories.forEach(subDirectory -> loadCsv(subDirectory, database));
            } catch (IOException e) {
                throw new RuntimeException("Unable to read input files");
            }
        } else if (directory.getFileName().toString().endsWith(SOURCE_EXTENSION)) {
            try (Reader reader = Files.newBufferedReader(directory)) {
                readCsv(reader, directory, database);
            } catch (IOException e) {
                throw new RuntimeException("Unable to reade input files");
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void readCsv(Reader reader, Path filePath, Database database) {
        try (CSVReader csvReader = new CSVReader(reader)) {
            if (Objects.nonNull(csvReader.peek())) {
                List<Integer> validColumns = validateColumns(csvReader.peek());
                Table table = createTable(filePath, validColumns.size());
                for (String[] values : csvReader) {
                    for (int i = 0; i < validColumns.size(); i++) {
                        Column<String> column = (Column<String>) table.get(COLUMN_PREFIX + (i + 1));
                        column.add(values[validColumns.get(i)]);
                    }
                }
                database.add(table);
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to read input files");
        }
    }

    private List<Integer> validateColumns(String[] columns) {
        List<Integer> validColumns = new ArrayList<>();
        for (int i = 0; i < columns.length; i++) {
            if (!columns[i].isEmpty()) {
                validColumns.add(i);
            }
        }
        return validColumns;
    }

    private Table createTable(Path source, int columnCount) {
        Table table = new GenericTable(TABLE_PREFIX + (this.tableCount + 1));
        this.tableCount += 1;
        table.addSource(source);
        IntStream.range(0, columnCount)
                .mapToObj(i -> COLUMN_PREFIX + (i + 1))
                .forEach(columnName -> table.add(new GenericColumn<String>(columnName)));
        return table;
    }

    private static class CsvRepositoryHolder {
        private static final CsvRepository CSV_REPOSITORY = new CsvRepository();
    }
}
