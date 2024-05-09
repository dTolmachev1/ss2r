package io.github.dtolmachev1.inference.rule;

import io.github.dtolmachev1.configuration.Configuration;
import io.github.dtolmachev1.configuration.XmlConfiguration;
import io.github.dtolmachev1.data.column.Column;
import io.github.dtolmachev1.data.column.ColumnType;
import io.github.dtolmachev1.data.column.ColumnTypeFactory;
import io.github.dtolmachev1.data.column.DoubleType;
import io.github.dtolmachev1.data.column.IntegerType;
import io.github.dtolmachev1.data.column.StringType;
import io.github.dtolmachev1.data.database.Database;
import io.github.dtolmachev1.data.table.Table;
import io.github.dtolmachev1.inference.analysis.Analysis;
import io.github.dtolmachev1.inference.analysis.AnalysisBuilderFactory;
import io.github.dtolmachev1.inference.analysis.ColumnTypeAnalysis;
import io.github.dtolmachev1.inference.validator.ColumnTypeValidator;
import io.github.dtolmachev1.inference.validator.ColumnValidator;
import io.github.dtolmachev1.inference.validator.ColumnValidatorBuilderFactory;

import java.util.Optional;

public class ColumnTypeInferenceRule implements InferenceRule {
    public static final String INFERENCE_RULE_NAME = "column-type";
    private final Configuration configuration;

    private ColumnTypeInferenceRule() {
        this.configuration = XmlConfiguration.newInstance();
    }

    public static ColumnTypeInferenceRule newInstance() {
        return ColumnTypeInferenceRuleHolder.COLUMN_TYPE_INFERENCE_RULE;
    }

    @Override
    public String name() {
        return INFERENCE_RULE_NAME;
    }

    @Override
    public Optional<Analysis> apply(Database database) {
        ColumnTypeAnalysis.ColumnTypeAnalysisBuilder columnTypeAnalysisBuilder = (ColumnTypeAnalysis.ColumnTypeAnalysisBuilder) AnalysisBuilderFactory.getAnalysisBuilder(INFERENCE_RULE_NAME);
        database.forEach(tableEntry -> determineColumnTypes(tableEntry.getValue(), columnTypeAnalysisBuilder));
        return Optional.of(columnTypeAnalysisBuilder.build());
    }

    @SuppressWarnings("unchecked")
    private void determineColumnTypes(Table table, ColumnTypeAnalysis.ColumnTypeAnalysisBuilder columnTypeAnalysisBuilder) {
        table.stream()
                .filter(columnEntry -> columnEntry.getValue().getType().name().equals(StringType.TYPE_NAME))
                .forEach(columnEntry -> columnTypeAnalysisBuilder.columnType(table.getName(), columnEntry.getKey(), determineColumnType((Column<String>) columnEntry.getValue())));
    }

    @SuppressWarnings("DuplicatedCode")
    private ColumnType determineColumnType(Column<String> column) {
        ColumnValidator integerValidator = ((ColumnTypeValidator.ColumnTypeValidatorBuilder) ColumnValidatorBuilderFactory.getColumnValidatorBuilder(INFERENCE_RULE_NAME))
                .column(column)
                .columnType(ColumnTypeFactory.getColumnType(IntegerType.TYPE_NAME))
                .build();
        ColumnValidator doubleValidator = ((ColumnTypeValidator.ColumnTypeValidatorBuilder) ColumnValidatorBuilderFactory.getColumnValidatorBuilder(INFERENCE_RULE_NAME))
                .column(column)
                .columnType(ColumnTypeFactory.getColumnType(IntegerType.TYPE_NAME))
                .build();
        int integerCount = Math.toIntExact(column.stream()
                .filter(entry -> integerValidator.isValid(entry.getKey()))
                .count());
        int doubleCount = Math.toIntExact(column.stream()
                .filter(entry -> doubleValidator.isValid(entry.getKey()))
                .count());
        if ((double) integerCount / column.size() >= this.configuration.typeThreshold() && integerCount >= doubleCount) {
            return ColumnTypeFactory.getColumnType(IntegerType.TYPE_NAME);
        }
        if ((double) doubleCount / column.size() >= this.configuration.typeThreshold()) {
            return ColumnTypeFactory.getColumnType(DoubleType.TYPE_NAME);
        }
        StringType stringType = (StringType) ColumnTypeFactory.getColumnType(StringType.TYPE_NAME);
        stringType.setMaxLength(column.stream()
                .map(entry -> entry.getValue().length())
                .max(Integer::compare)
                .orElse(0));
        return stringType;
    }

    private static class ColumnTypeInferenceRuleHolder {
        private static final ColumnTypeInferenceRule COLUMN_TYPE_INFERENCE_RULE = new ColumnTypeInferenceRule();
    }
}
