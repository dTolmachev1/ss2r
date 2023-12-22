package io.github.dtolmachev1.ss2r.repository.source;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import io.github.dtolmachev1.ss2r.configuration.Configuration;
import io.github.dtolmachev1.ss2r.configuration.XmlConfiguration;
import io.github.dtolmachev1.ss2r.data.column.Column;
import io.github.dtolmachev1.ss2r.data.column.SimpleColumn;
import io.github.dtolmachev1.ss2r.data.database.Database;
import io.github.dtolmachev1.ss2r.data.database.SimpleDatabase;
import io.github.dtolmachev1.ss2r.data.table.SimpleTable;
import io.github.dtolmachev1.ss2r.data.table.Table;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class CsvRepository implements SourceRepository {
    private Path path;
    private final Database database;
    private final Map<String, List<String>> tableNames;
    private final Map<String, Integer> resolvedTableNames;
    private final Configuration configuration;

    private CsvRepository() {
        this.database = new SimpleDatabase();
        this.tableNames = new HashMap<>();
        this.resolvedTableNames = new HashMap<>();
        this.configuration = XmlConfiguration.newInstance();
    }

    public static CsvRepository newInstance(Path path) {
        CsvRepositoryHolder.CSV_REPOSITORY.path = path;
        CsvRepositoryHolder.CSV_REPOSITORY.database.setName(getFileName(CsvRepositoryHolder.CSV_REPOSITORY.path));
        return CsvRepositoryHolder.CSV_REPOSITORY;
    }

    @Override
    public Database load() {
        loadCsv(this.path);
        normalize();
        return this.database;
    }

    private void loadCsv(Path directory) {
        if (Files.isDirectory(directory)) {
            try (DirectoryStream<Path> directories = Files.newDirectoryStream(directory)) {
                directories.forEach(this::loadCsv);
            } catch (IOException e) {
                throw new RuntimeException("Unable to read input files");
            }
        } else if (directory.getFileName().toString().endsWith(".csv")) {
            try (Reader reader = Files.newBufferedReader(directory)) {
                readCsv(getFileName(directory), reader);
            } catch (IOException e) {
                throw new RuntimeException("Unable to read input files");
            }
        }
    }

    private void normalize() {
        for (Entry<String, Table> entry : this.database.stream().toList()) {
            resolveTableName(entry.getValue());
            if (!entry.getValue().name().equals(entry.getKey())) {
                this.database.remove(entry.getKey());
                this.database.add(entry.getValue());
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void readCsv(String fileName, Reader reader) {
        try (CSVReader csvReader = new CSVReader(reader)) {
            String[] columns = csvReader.readNext();
            if (Objects.nonNull(columns) && Objects.nonNull(csvReader.peek())) {
                Table table = createTable(fileName, Arrays.stream(columns).skip(1).toList());
                for (String[] values : csvReader) {
                    for (int i = 1; i < values.length; i++) {
                        Column<String> column = (Column<String>) table.get(columns[i]);
                        column.add(values[i]);
                    }
                }
            }
        } catch (CsvValidationException | IOException e) {
            throw new RuntimeException("Unable to read input files");
        }
    }

    private Table createTable(String name, List<String> columns) {
        if (!this.configuration.mergeSimilarTables()) {
            Table table = new SimpleTable(name);
            columns.forEach(column -> table.add(new SimpleColumn<String>(column)));
            this.database.add(table);
            return table;
        }
        List<Table> candidateTables = this.database.stream()
                .map(Entry::getValue)
                .filter(table -> table.stream().map(Entry::getKey).toList().equals(columns))
                .toList();
        Table table = candidateTables.size() == 1 ? candidateTables.get(0) : new SimpleTable(name);
        if (table.isEmpty()) {
            columns.forEach(column -> table.add(new SimpleColumn<String>(column)));
            this.database.add(table);
        }
        this.tableNames.computeIfAbsent(table.name(), k -> new ArrayList<>()).add(name);
        return table;
    }

    private void resolveTableName(Table table) {
        String tableName = processTableName(this.tableNames.get(table.name()).stream().reduce(CsvRepository::longestCommonSubstring).orElse(table.name()));
        if (this.resolvedTableNames.merge(tableName, 1, Integer::sum) > 1) {
            table.setName(tableName + "_" + (this.resolvedTableNames.get(tableName) - 1));
        } else {
            table.setName(tableName);
        }
    }

    private static String getFileName(Path filePath) {
        String fileName = filePath.getFileName().toString();
        int index = fileName.lastIndexOf(".");
        return index != -1 ? fileName.substring(0, index) : fileName;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static String processTableName(String tableName) {
        Pattern pattern = Pattern.compile("^([^\\d\\p{L}]*\\d+)*[^\\d\\p{L}]*(\\p{L}+\\P{L}*)*$");
        Matcher matcher = pattern.matcher(tableName);
        matcher.find();
        if (Objects.nonNull(matcher.group(1)) && Objects.nonNull(matcher.group(2))) {
            return matcher.replaceFirst("$2_$1");
        } else if (Objects.nonNull(matcher.group(1))) {
            return matcher.replaceFirst("TABLE_$0");
        } else {
            return tableName;
        }
    }

    @SuppressWarnings("DuplicateExpressions")
    private static String longestCommonSubstring(String a, String b) {
        List<List<Integer>> dynamicTable = Stream
                .generate(() -> Stream.generate(() -> 0).limit(b.length() + 1).collect(ArrayList<Integer>::new, ArrayList<Integer>::add, ArrayList<Integer>::addAll))
                .limit(2)
                .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
        String result = "";
        for (int i = 1; i <= a.length(); i++) {
            for (int j = 1; j <= b.length(); j++) {
                if (a.charAt(i - 1) == b.charAt(j - 1) && (dynamicTable.get((i - 1) % 2).get(j - 1) != 0 || Character.isLetter(b.charAt(j - 1)))) {
                    dynamicTable.get(i % 2).set(j, dynamicTable.get((i - 1) % 2).get(j - 1) + 1);
                    if (dynamicTable.get(i % 2).get(j) > result.length() && Character.isLetter(b.charAt(j - 1))) {
                        result = b.substring(j - dynamicTable.get(i % 2).get(j), j);
                    }
                } else {
                    dynamicTable.get((i % 2)).set(j, 0);
                }
            }
        }
        return result;
    }

    private static class CsvRepositoryHolder {
        private static final CsvRepository CSV_REPOSITORY = new CsvRepository();
    }
}
