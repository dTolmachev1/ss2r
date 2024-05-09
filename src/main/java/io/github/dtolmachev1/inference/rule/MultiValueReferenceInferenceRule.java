package io.github.dtolmachev1.inference.rule;

import io.github.dtolmachev1.configuration.Configuration;
import io.github.dtolmachev1.configuration.XmlConfiguration;
import io.github.dtolmachev1.data.column.Column;
import io.github.dtolmachev1.data.column.ColumnType;
import io.github.dtolmachev1.data.column.ColumnTypeFactory;
import io.github.dtolmachev1.data.column.DoubleType;
import io.github.dtolmachev1.data.column.GenericColumn;
import io.github.dtolmachev1.data.column.IntegerType;
import io.github.dtolmachev1.data.column.StringType;
import io.github.dtolmachev1.data.database.Database;
import io.github.dtolmachev1.data.table.Table;
import io.github.dtolmachev1.inference.analysis.Analysis;
import io.github.dtolmachev1.inference.analysis.AnalysisBuilderFactory;
import io.github.dtolmachev1.inference.analysis.MultiValueReferenceAnalysis;
import io.github.dtolmachev1.inference.validator.ColumnTypeValidator;
import io.github.dtolmachev1.inference.validator.ColumnValidator;
import io.github.dtolmachev1.inference.validator.ColumnValidatorBuilderFactory;
import io.github.dtolmachev1.inference.validator.MultiValueReferenceValidator;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class MultiValueReferenceInferenceRule implements InferenceRule {
    public static final String INFERENCE_RULE_NAME = "multi-value-reference";
    private static final String NEW_TABLE_NAME_SUFFIX = "_to_";
    private static final String NEW_ID_COLUMN_NAME = "id";
    private static final String NEW_COLUMN_NAME_SUFFIX = "_id";
    private final Configuration configuration;

    private MultiValueReferenceInferenceRule() {
        this.configuration = XmlConfiguration.newInstance();
    }

    public static MultiValueReferenceInferenceRule newInstance() {
        return MultiValueReferenceInferenceRuleHolder.MULTI_VALUE_REFERENCE_INFERENCE_RULE;
    }

    @Override
    public String name() {
        return INFERENCE_RULE_NAME;
    }

    @Override
    public Optional<Analysis> apply(Database database) {
        MultiValueReferenceAnalysis.MultiValueReferenceAnalysisBuilder multiValueReferenceAnalysisBuilder = (MultiValueReferenceAnalysis.MultiValueReferenceAnalysisBuilder) AnalysisBuilderFactory.getAnalysisBuilder(INFERENCE_RULE_NAME);
        int multiValueReferenceCount = determineMultiValueReferences(database, multiValueReferenceAnalysisBuilder);
        return multiValueReferenceCount > 0 ? Optional.of(multiValueReferenceAnalysisBuilder.build()) : Optional.empty();
    }

    private int determineMultiValueReferences(Database database, MultiValueReferenceAnalysis.MultiValueReferenceAnalysisBuilder multiValueReferenceAnalysisBuilder) {
        Map<String, Column<?>> columnsWithUniqueConstraint = database.stream()
                .map(entry -> new AbstractMap.SimpleImmutableEntry<>(entry.getKey(), entry.getValue().columnWithUniqueConstraint()))
                .filter(entry -> entry.getValue().isPresent())
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().get()));
        return database.stream()
                .map(entry -> determineMultiValueReferencingTable(entry.getValue(), columnsWithUniqueConstraint, multiValueReferenceAnalysisBuilder))
                .reduce(0, Integer::sum);
    }

    @SuppressWarnings("unchecked")
    private int determineMultiValueReferencingTable(Table referencingTable, Map<String, Column<?>> columnsWithUniqueConstraint, MultiValueReferenceAnalysis.MultiValueReferenceAnalysisBuilder multiValueReferenceAnalysisBuilder) {
        return Math.toIntExact(referencingTable.stream()
                .filter(entry -> entry.getValue().getType().name().equals(StringType.TYPE_NAME) && ((StringType) entry.getValue().getType()).getMaxLength() >= this.configuration.multiValueReferenceLength() && Optional.ofNullable(columnsWithUniqueConstraint.get(referencingTable.getName())).map(column -> !column.getName().equals(entry.getKey())).orElse(true))
                .map(entry -> determineMultiValueReferencingColumn(referencingTable.getName(), (Column<String>) entry.getValue(), columnsWithUniqueConstraint, multiValueReferenceAnalysisBuilder))
                .filter(value -> value)
                .count());
    }

    @SuppressWarnings("DuplicatedCode")
    private boolean determineMultiValueReferencingColumn(String referencingTableName, Column<String> multiValueReferencingColumn, Map<String, Column<?>> columnsWithUniqueConstraint, MultiValueReferenceAnalysis.MultiValueReferenceAnalysisBuilder multiValueReferenceAnalysisBuilder) {
        Optional<String> multiValueReferenceSeparator = determineMultiValueReferenceSeparator(multiValueReferencingColumn);
        if (multiValueReferenceSeparator.isEmpty()) {
            return false;
        }
        Column<?> referencingColumn = createReferencingColumn(multiValueReferencingColumn, multiValueReferenceSeparator.get());
        Map<String, Integer> referenceCount = columnsWithUniqueConstraint.entrySet().stream()
                .filter(entry -> !referencingTableName.equals(entry.getKey()) && referencingColumn.getType().name().equals(entry.getValue().getType().name()))
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> countReference(referencingColumn, entry.getValue())));
        Optional<String> referenceCandidate = determineCandidate(referencingColumn, referenceCount);
        if (referenceCandidate.isPresent()) {
            multiValueReferenceAnalysisBuilder.multiValueReference(referencingTableName, referencingColumn.getName(), multiValueReferenceSeparator.get(), referenceCandidate.get(), columnsWithUniqueConstraint.get(referenceCandidate.get()).getName(), referencingTableName + NEW_TABLE_NAME_SUFFIX + referenceCandidate.get(), NEW_ID_COLUMN_NAME, referencingTableName + NEW_COLUMN_NAME_SUFFIX, referenceCandidate.get() + NEW_COLUMN_NAME_SUFFIX);
            return true;
        }
        return false;
    }

    private Optional<String> determineMultiValueReferenceSeparator(Column<String> multiValueReferencingColumn) {
        return this.configuration.multiValueReferenceSeparators().stream()
                .map(multiValueReferenceSeparator -> new AbstractMap.SimpleImmutableEntry<>(multiValueReferenceSeparator, countMultiValueReferences(multiValueReferencingColumn, multiValueReferenceSeparator)))
                .filter(entry -> entry.getValue() > 0)
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey);
    }

    private int countMultiValueReferences(Column<String> multiValueReferencingColumn, String multiValueReferenceSeparator) {
        int maxSplitCount = 0;
        for (Map.Entry<Integer, String> columnEntry : multiValueReferencingColumn) {
            int splitCount = columnEntry.getValue().split(Pattern.quote(multiValueReferenceSeparator)).length;
            if (!columnEntry.getValue().isEmpty() && splitCount < this.configuration.multiValueReferenceCount()) {
                return 0;
            }
            if (splitCount > maxSplitCount) {
                maxSplitCount = splitCount;
            }
        }
        return maxSplitCount;
    }

    private Column<?> createReferencingColumn(Column<String> multiValueReferencingColumn, String multiValueReferenceSeparator) {
        Column<String> stringColumn = createStringColumn(multiValueReferencingColumn, multiValueReferenceSeparator);
        ColumnType columnType = determineColumnType(stringColumn);
        validateColumn(stringColumn, columnType);
        return createColumn(stringColumn, columnType);
    }

    private Column<String> createStringColumn(Column<String> multiValueReferencingColumn, String multiValueReferenceSeparator) {
        Column<String> stringColumn = new GenericColumn<>(multiValueReferencingColumn.getName(), ColumnTypeFactory.getColumnType(StringType.TYPE_NAME));
        for (Map.Entry<Integer, String> columnEntry : multiValueReferencingColumn) {
            for (String splitValue : columnEntry.getValue().split(Pattern.quote(multiValueReferenceSeparator))) {
                stringColumn.add(splitValue);
            }
        }
        return stringColumn;
    }

    @SuppressWarnings("DuplicatedCode")
    private ColumnType determineColumnType(Column<String> column) {
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
                .orElse(1));
        return stringType;
    }

    private void validateColumn(Column<String> sourceColumn, ColumnType columnType) {
        ColumnValidator columnValidator = ((ColumnTypeValidator.ColumnTypeValidatorBuilder) ColumnValidatorBuilderFactory.getColumnValidatorBuilder(ColumnTypeValidator.VALIDATOR_NAME))
                .column(sourceColumn)
                .columnType(columnType)
                .build();
        Iterator<Map.Entry<Integer, String>> iterator = sourceColumn.iterator();
        while (iterator.hasNext()) {
            if (!columnValidator.isValid(iterator.next().getKey())) {
                iterator.remove();
            }
        }
    }

    @SuppressWarnings("DuplicatedCode")
    private Column<?> createColumn(Column<String> sourceColumn, ColumnType columnType) {
        if (columnType.name().equals(IntegerType.TYPE_NAME)) {
            Column<Integer> newColumn = new GenericColumn<>(sourceColumn.getName(), columnType);
            sourceColumn.forEach(entry -> newColumn.add(entry.getKey(), Integer.valueOf(entry.getValue())));
            return newColumn;
        }
        if (columnType.name().equals(DoubleType.TYPE_NAME)) {
            Column<Double> newColumn = new GenericColumn<>(sourceColumn.getName(), columnType);
            sourceColumn.forEach(entry -> newColumn.add(entry.getKey(), Double.valueOf(entry.getValue())));
            return newColumn;
        }
        if (columnType.name().equals(StringType.TYPE_NAME)) {
            StringType stringType = (StringType) columnType;
            stringType.setMaxLength(sourceColumn.stream()
                    .map(entry -> entry.getValue().length())
                    .max(Integer::compare)
                    .orElse(0));
        }
        sourceColumn.setType(columnType);
        return sourceColumn;
    }

    private int countReference(Column<?> referencingColumn, Column<?> referencedColumn) {
        ColumnValidator columnValidator = ((MultiValueReferenceValidator.MultiValueReferenceValidatorBuilder) ColumnValidatorBuilderFactory.getColumnValidatorBuilder(INFERENCE_RULE_NAME))
                .referencingColumn(referencingColumn)
                .referencedColumn(referencedColumn)
                .build();
        return Math.toIntExact(referencingColumn.stream()
                .filter(entry -> columnValidator.isValid(entry.getKey()))
                .count());
    }

    private Optional<String> determineCandidate(Column<?> referencingColumn, Map<String, Integer> candidatesCount) {
        return candidatesCount.entrySet().stream()
                .filter(entry -> (double) entry.getValue() / referencingColumn.size() >= this.configuration.multiValueReferenceThreshold())
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey);
    }

    private static class MultiValueReferenceInferenceRuleHolder {
        private static final MultiValueReferenceInferenceRule MULTI_VALUE_REFERENCE_INFERENCE_RULE = new MultiValueReferenceInferenceRule();
    }
}
