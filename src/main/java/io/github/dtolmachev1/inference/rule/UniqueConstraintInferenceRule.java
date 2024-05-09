package io.github.dtolmachev1.inference.rule;

import io.github.dtolmachev1.configuration.Configuration;
import io.github.dtolmachev1.configuration.XmlConfiguration;
import io.github.dtolmachev1.data.column.Column;
import io.github.dtolmachev1.data.column.DoubleType;
import io.github.dtolmachev1.data.column.IntegerType;
import io.github.dtolmachev1.data.column.StringType;
import io.github.dtolmachev1.data.database.Database;
import io.github.dtolmachev1.data.table.Table;
import io.github.dtolmachev1.inference.analysis.Analysis;
import io.github.dtolmachev1.inference.analysis.AnalysisBuilderFactory;
import io.github.dtolmachev1.inference.analysis.UniqueConstraintAnalysis;
import io.github.dtolmachev1.inference.validator.UniqueConstraintValidator;
import io.github.dtolmachev1.inference.validator.ColumnValidator;
import io.github.dtolmachev1.inference.validator.ColumnValidatorBuilderFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class UniqueConstraintInferenceRule implements InferenceRule {
    public static final String INFERENCE_RULE_NAME = "unique-constraint";
    private final Configuration configuration;

    private UniqueConstraintInferenceRule() {
        this.configuration = XmlConfiguration.newInstance();
    }

    public static UniqueConstraintInferenceRule newInstance() {
        return UniqueConstraintInferenceRuleHolder.UNIQUE_CONSTRAINT_INFERENCE_RULE;
    }

    @Override
    public String name() {
        return INFERENCE_RULE_NAME;
    }

    @Override
    public Optional<Analysis> apply(Database database) {
        UniqueConstraintAnalysis.UniqueConstraintAnalysisBuilder uniqueConstraintAnalysisBuilder = (UniqueConstraintAnalysis.UniqueConstraintAnalysisBuilder) AnalysisBuilderFactory.getAnalysisBuilder(INFERENCE_RULE_NAME);
        int uniqueConstraintCount = Math.toIntExact(database.stream()
                .map(entry -> determineUnique(entry.getValue(), uniqueConstraintAnalysisBuilder))
                .filter(value -> value)
                .count());
        return uniqueConstraintCount > 0 ? Optional.of(uniqueConstraintAnalysisBuilder.build()) : Optional.empty();
    }

    private boolean determineUnique(Table table, UniqueConstraintAnalysis.UniqueConstraintAnalysisBuilder uniqueConstraintAnalysisBuilder) {
        Map<String, Integer> stringCount = table.stream()
                .filter(entry -> entry.getValue().getType().name().equals(StringType.TYPE_NAME) && ((StringType) entry.getValue().getType()).getMaxLength() < this.configuration.multiValueReferenceLength())
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> countUnique(entry.getValue())));
        Map<String, Integer> integerCount = table.stream()
                .filter(entry -> entry.getValue().getType().name().equals(IntegerType.TYPE_NAME))
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> countUnique(entry.getValue())));
        Map<String, Integer> doubleCount = table.stream()
                .filter(entry -> entry.getValue().getType().name().equals(DoubleType.TYPE_NAME))
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> countUnique(entry.getValue())));
        Optional<String> stringCandidate = determineCandidate(table, stringCount);
        Optional<String> integerCandidate = determineCandidate(table, integerCount);
        Optional<String> doubleCandidate = determineCandidate(table, doubleCount);
        if (integerCandidate.isPresent() && integerCount.get(integerCandidate.get()).equals(Collections.min(List.of(stringCandidate.map(stringCount::get).orElse(Integer.MAX_VALUE), integerCount.get(integerCandidate.get()), doubleCandidate.map(doubleCount::get).orElse(Integer.MAX_VALUE))))) {
            uniqueConstraintAnalysisBuilder.uniqueConstraint(table.getName(), integerCandidate.get());
            return true;
        }
        if (stringCandidate.isPresent() && stringCount.get(stringCandidate.get()) <= doubleCandidate.map(doubleCount::get).orElse(Integer.MAX_VALUE)) {
            uniqueConstraintAnalysisBuilder.uniqueConstraint(table.getName(), stringCandidate.get());
            return true;
        }
        if (doubleCandidate.isPresent()) {
            uniqueConstraintAnalysisBuilder.uniqueConstraint(table.getName(), doubleCandidate.get());
            return true;
        }
        return false;
    }

    private int countUnique(Column<?> column) {
        ColumnValidator columnValidator = ((UniqueConstraintValidator.UniqueConstraintValidatorBuilder) ColumnValidatorBuilderFactory.getColumnValidatorBuilder(INFERENCE_RULE_NAME))
                .column(column)
                .build();
        return Math.toIntExact(column.stream()
                .filter(entry -> columnValidator.isValid(entry.getKey()))
                .count());
    }

    private Optional<String> determineCandidate(Table table, Map<String, Integer> candidatesCount) {
        return candidatesCount.entrySet().stream()
                .filter(entry -> (double) entry.getValue() / table.get(entry.getKey()).size() >= this.configuration.uniqueThreshold())
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey);
    }

    private static class UniqueConstraintInferenceRuleHolder {
        private static final UniqueConstraintInferenceRule UNIQUE_CONSTRAINT_INFERENCE_RULE = new UniqueConstraintInferenceRule();
    }
}
