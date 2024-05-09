package io.github.dtolmachev1.inference.rule;

import io.github.dtolmachev1.configuration.Configuration;
import io.github.dtolmachev1.configuration.XmlConfiguration;
import io.github.dtolmachev1.data.column.Column;
import io.github.dtolmachev1.data.column.StringType;
import io.github.dtolmachev1.data.database.Database;
import io.github.dtolmachev1.data.table.Table;
import io.github.dtolmachev1.inference.analysis.Analysis;
import io.github.dtolmachev1.inference.analysis.AnalysisBuilderFactory;
import io.github.dtolmachev1.inference.analysis.ReferenceConstraintAnalysis;
import io.github.dtolmachev1.inference.validator.ReferenceConstraintValidator;
import io.github.dtolmachev1.inference.validator.ColumnValidator;
import io.github.dtolmachev1.inference.validator.ColumnValidatorBuilderFactory;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class ReferenceConstraintInferenceRule implements InferenceRule {
    public static final String INFERENCE_RULE_NAME = "reference-constraint";
    private final Configuration configuration;

    private ReferenceConstraintInferenceRule() {
        this.configuration = XmlConfiguration.newInstance();
    }

    public static ReferenceConstraintInferenceRule newInstance() {
        return ReferenceConstraintInferenceRuleHolder.REFERENCE_CONSTRAINT_INFERENCE_RULE;
    }

    @Override
    public String name() {
        return INFERENCE_RULE_NAME;
    }

    @Override
    public Optional<Analysis> apply(Database database) {
        ReferenceConstraintAnalysis.ReferenceConstraintAnalysisBuilder referenceConstraintAnalysisBuilder = (ReferenceConstraintAnalysis.ReferenceConstraintAnalysisBuilder) AnalysisBuilderFactory.getAnalysisBuilder(INFERENCE_RULE_NAME);
        int referenceConstraintCount = determineReference(database, referenceConstraintAnalysisBuilder);
        return referenceConstraintCount > 0 ? Optional.of(referenceConstraintAnalysisBuilder.build()) : Optional.empty();
    }

    private int determineReference(Database database, ReferenceConstraintAnalysis.ReferenceConstraintAnalysisBuilder referenceConstraintAnalysisBuilder) {
        Map<String, Column<?>> columnsWithUniqueConstraint = database.stream()
                .map(entry -> new AbstractMap.SimpleImmutableEntry<>(entry.getKey(), entry.getValue().columnWithUniqueConstraint()))
                .filter(entry -> entry.getValue().isPresent())
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().get()));
        return database.stream()
                .map(entry -> determineReferencingTable(entry.getValue(), columnsWithUniqueConstraint, referenceConstraintAnalysisBuilder))
                .reduce(0, Integer::sum);
    }

    private int determineReferencingTable(Table referencingTable, Map<String, Column<?>> columnsWithUniqueConstraint, ReferenceConstraintAnalysis.ReferenceConstraintAnalysisBuilder referenceConstraintAnalysisBuilder) {
        return Math.toIntExact(referencingTable.stream()
                .filter(entry -> (!entry.getValue().getType().name().equals(StringType.TYPE_NAME) || ((StringType) entry.getValue().getType()).getMaxLength() < this.configuration.multiValueReferenceLength()) && Optional.ofNullable(columnsWithUniqueConstraint.get(referencingTable.getName())).map(column -> !column.getName().equals(entry.getKey())).orElse(true))
                .map(entry -> determineReferencingColumn(referencingTable.getName(), entry.getValue(), columnsWithUniqueConstraint, referenceConstraintAnalysisBuilder))
                .filter(value -> value)
                .count());
    }

    @SuppressWarnings("DuplicatedCode")
    private boolean determineReferencingColumn(String referencingTableName, Column<?> referencingColumn, Map<String, Column<?>> columnsWithUniqueConstraint, ReferenceConstraintAnalysis.ReferenceConstraintAnalysisBuilder referenceConstraintAnalysisBuilder) {
        Map<String, Integer> referenceCount = columnsWithUniqueConstraint.entrySet().stream()
                .filter(entry -> !referencingTableName.equals(entry.getKey()) && referencingColumn.getType().name().equals(entry.getValue().getType().name()))
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> countReference(referencingColumn, entry.getValue())));
        Optional<String> referenceCandidate = determineCandidate(referencingColumn, referenceCount);
        if (referenceCandidate.isPresent()) {
            referenceConstraintAnalysisBuilder.referenceConstraint(referencingTableName, referencingColumn.getName(), referenceCandidate.get(), columnsWithUniqueConstraint.get(referenceCandidate.get()).getName());
            return true;
        }
        return false;
    }

    private int countReference(Column<?> referencingColumn, Column<?> referencedColumn) {
        ColumnValidator columnValidator = ((ReferenceConstraintValidator.ReferenceConstraintValidatorBuilder) ColumnValidatorBuilderFactory.getColumnValidatorBuilder(INFERENCE_RULE_NAME))
                .referencingColumn(referencingColumn)
                .referencedColumn(referencedColumn)
                .build();
        return Math.toIntExact(referencingColumn.stream()
                .filter(entry -> columnValidator.isValid(entry.getKey()))
                .count());
    }

    private Optional<String> determineCandidate(Column<?> referencingColumn, Map<String, Integer> candidatesCount) {
        return candidatesCount.entrySet().stream()
                .filter(entry -> (double) entry.getValue() / referencingColumn.size() >= this.configuration.referenceThreshold())
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey);
    }

    private static class ReferenceConstraintInferenceRuleHolder {
        private static final ReferenceConstraintInferenceRule REFERENCE_CONSTRAINT_INFERENCE_RULE = new ReferenceConstraintInferenceRule();
    }
}
