package io.github.dtolmachev1.inference.rule;

import io.github.dtolmachev1.configuration.Configuration;
import io.github.dtolmachev1.configuration.XmlConfiguration;
import io.github.dtolmachev1.data.column.Column;
import io.github.dtolmachev1.data.column.ColumnTypeFactory;
import io.github.dtolmachev1.data.column.IntegerType;
import io.github.dtolmachev1.data.column.StringType;
import io.github.dtolmachev1.data.database.Database;
import io.github.dtolmachev1.data.table.Table;
import io.github.dtolmachev1.inference.analysis.Analysis;
import io.github.dtolmachev1.inference.analysis.AnalysisBuilderFactory;
import io.github.dtolmachev1.inference.analysis.ColumnNameAnalysis;
import io.github.dtolmachev1.inference.validator.ColumnTypeValidator;
import io.github.dtolmachev1.inference.validator.ColumnValidator;
import io.github.dtolmachev1.inference.validator.ColumnValidatorBuilderFactory;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class ColumnNameInferenceRule implements InferenceRule {
    public static final String INFERENCE_RULE_NAME = "column-name";
    private final Configuration configuration;

    private ColumnNameInferenceRule() {
        this.configuration = XmlConfiguration.newInstance();
    }

    public static ColumnNameInferenceRule newInstance() {
        return ColumnNameInferenceRuleHolder.COLUMN_NAME_INFERENCE_RULE;
    }

    @Override
    public String name() {
        return INFERENCE_RULE_NAME;
    }

    @Override
    public Optional<Analysis> apply(Database database) {
        ColumnNameAnalysis.ColumnNameAnalysisBuilder columnNameAnalysisBuilder = (ColumnNameAnalysis.ColumnNameAnalysisBuilder) AnalysisBuilderFactory.getAnalysisBuilder(INFERENCE_RULE_NAME);
        int columnNameCount = database.stream()
                .map(entry -> determineColumnNames(entry.getValue(), columnNameAnalysisBuilder))
                .reduce(0, Integer::sum);
        return columnNameCount != 0 ? Optional.of(columnNameAnalysisBuilder.build()) : Optional.empty();
    }

    @SuppressWarnings("unchecked")
    private int determineColumnNames(Table table, ColumnNameAnalysis.ColumnNameAnalysisBuilder columnNameAnalysisBuilder) {
        Map<String, String> columnNames = table.stream()
                .filter(entry -> entry.getValue().getType().name().equals(StringType.TYPE_NAME))
                .map(entry -> new AbstractMap.SimpleImmutableEntry<>(entry.getKey(), determineColumnName((Column<String>) entry.getValue())))
                        .filter(entry -> entry.getValue().isPresent())
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().get()));
        if (columnNames.size() == table.size()) {
            columnNames.forEach((sourceColumnName, newColumnName) -> columnNameAnalysisBuilder.columnName(table.getName(), sourceColumnName, newColumnName));
            return table.size();
        }
        return 0;
    }

    @SuppressWarnings("DuplicatedCode")
    private Optional<String> determineColumnName(Column<String> column) {
        ColumnValidator integerValidator = ((ColumnTypeValidator.ColumnTypeValidatorBuilder) ColumnValidatorBuilderFactory.getColumnValidatorBuilder(ColumnTypeValidator.VALIDATOR_NAME))
                .column(column)
                .columnType(ColumnTypeFactory.getColumnType(IntegerType.TYPE_NAME))
                .build();
        ColumnValidator doubleValidator = ((ColumnTypeValidator.ColumnTypeValidatorBuilder) ColumnValidatorBuilderFactory.getColumnValidatorBuilder(ColumnTypeValidator.VALIDATOR_NAME))
                .column(column)
                .columnType(ColumnTypeFactory.getColumnType(IntegerType.TYPE_NAME))
                .build();
        int integerCount = Math.toIntExact(column.stream()
                .filter(entry -> integerValidator.isValid(entry.getKey()))
                .count());
        int doubleCount = Math.toIntExact(column.stream()
                .filter(entry -> doubleValidator.isValid(entry.getKey()))
                .count());
        int id = column.stream().map(Map.Entry::getKey).findFirst().orElse(0);
        if ((double) integerCount / column.size() >= this.configuration.typeThreshold() && integerCount >= doubleCount) {
            return !integerValidator.isValid(id) ? Optional.of(column.get(id)) : Optional.empty();
        } else if ((double) doubleCount / column.size() >= this.configuration.typeThreshold()) {
            return !doubleValidator.isValid(id) ? Optional.of(column.get(id)) : Optional.empty();
        } else {
            return Optional.of(column.get(id));
        }
    }

    private static class ColumnNameInferenceRuleHolder {
        private static final ColumnNameInferenceRule COLUMN_NAME_INFERENCE_RULE = new ColumnNameInferenceRule();
    }
}
