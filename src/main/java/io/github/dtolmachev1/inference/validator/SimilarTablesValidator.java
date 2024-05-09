package io.github.dtolmachev1.inference.validator;

import io.github.dtolmachev1.data.table.Table;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SimilarTablesValidator implements TableValidator {
    public static final String VALIDATOR_NAME = "similar-tables";
    private final Set<String> similarColumns;

    private SimilarTablesValidator() {
        this.similarColumns = new HashSet<>();
    }

    public SimilarTablesValidator(List<Table> similarTables) {
        this();
        initializeSimilarColumns(similarTables);
    }

    @Override
    public String name() {
        return VALIDATOR_NAME;
    }

    @Override
    public boolean isValid(String columnName) {
        return this.similarColumns.contains(columnName);
    }

    private void initializeSimilarColumns(List<Table> similarTables) {
        if (!similarTables.isEmpty()) {
            similarTables.get(0).forEach(entry -> this.similarColumns.add(entry.getKey()));
        }
        similarTables.stream()
                .skip(1)
                .map(table -> table.stream().map(Map.Entry::getKey).collect(Collectors.toSet()))
                .forEach(this.similarColumns::retainAll);
    }

    public static SimilarTablesValidatorBuilder builder() {
        return new SimilarTablesValidatorBuilder();
    }

    public static class SimilarTablesValidatorBuilder implements TableValidatorBuilder {
        private final SimilarTablesValidator similarTablesValidator;

        private SimilarTablesValidatorBuilder() {
            this.similarTablesValidator = new SimilarTablesValidator();
        }

        public SimilarTablesValidatorBuilder similarTables(List<Table> similarTables) {
            this.similarTablesValidator.initializeSimilarColumns(similarTables);
            return this;
        }

        @Override
        public SimilarTablesValidator build() {
            return this.similarTablesValidator;
        }
    }
}
