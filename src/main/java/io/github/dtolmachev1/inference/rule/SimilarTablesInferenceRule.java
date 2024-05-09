package io.github.dtolmachev1.inference.rule;

import io.github.dtolmachev1.configuration.Configuration;
import io.github.dtolmachev1.configuration.XmlConfiguration;
import io.github.dtolmachev1.data.database.Database;
import io.github.dtolmachev1.data.table.Table;
import io.github.dtolmachev1.inference.analysis.Analysis;
import io.github.dtolmachev1.inference.analysis.AnalysisBuilderFactory;
import io.github.dtolmachev1.inference.analysis.SimilarTablesAnalysis;
import io.github.dtolmachev1.inference.validator.SimilarTablesValidator;
import io.github.dtolmachev1.inference.validator.TableValidator;
import io.github.dtolmachev1.inference.validator.TableValidatorBuilderFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class SimilarTablesInferenceRule implements InferenceRule {
    public static final String INFERENCE_RULE_NAME = "similar-tables";
    private final Configuration configuration;

    private SimilarTablesInferenceRule() {
        this.configuration = XmlConfiguration.newInstance();
    }

    public static SimilarTablesInferenceRule newInstance() {
        return SimilarTablesInferenceRuleHolder.SIMILAR_TABLES_INFERENCE_RULE;
    }

    @Override
    public String name() {
        return INFERENCE_RULE_NAME;
    }

    @Override
    public Optional<Analysis> apply(Database database) {
        if (!this.configuration.mergeSimilarTables()) {
            return Optional.empty();
        }
        SimilarTablesAnalysis.SimilarTablesAnalysisBuilder similarTablesAnalysisBuilder = (SimilarTablesAnalysis.SimilarTablesAnalysisBuilder) AnalysisBuilderFactory.getAnalysisBuilder(INFERENCE_RULE_NAME);
        int similarTablesCount = determineSimilarTables(database, similarTablesAnalysisBuilder);
        return similarTablesCount > 0 ? Optional.of(similarTablesAnalysisBuilder.build()) : Optional.empty();
    }

    private int determineSimilarTables(Database database, SimilarTablesAnalysis.SimilarTablesAnalysisBuilder similarTablesAnalysisBuilder) {
        Map<String, Table> unresolvedTables = database.stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        int similarTablesCount = 0;
        while (!unresolvedTables.isEmpty()) {
            similarTablesCount += determineSimilarTable(unresolvedTables, similarTablesAnalysisBuilder);
        }
        return similarTablesCount;
    }

    private int determineSimilarTable(Map<String, Table> unresolvedTables, SimilarTablesAnalysis.SimilarTablesAnalysisBuilder similarTablesAnalysisBuilder) {
        SortedSet<String> candidateTableNames = new TreeSet<>();
        LinkedList<Table> candidateTables = new LinkedList<>();
        Iterator<Map.Entry<String, Table>> iterator = unresolvedTables.entrySet().iterator();
        if (iterator.hasNext()) {
            Map.Entry<String, Table> currentTableEntry = iterator.next();
            candidateTables.push(currentTableEntry.getValue());
            candidateTableNames.add(currentTableEntry.getKey());
            iterator.remove();
        }
        while (iterator.hasNext()) {
            Map.Entry<String, Table> currentTableEntry = iterator.next();
            candidateTables.push(currentTableEntry.getValue());
            if (isSimilarTables(candidateTables)) {
                candidateTableNames.add(currentTableEntry.getKey());
                iterator.remove();
            } else {
                candidateTables.pop();
            }
        }
        if (candidateTableNames.size() > 1) {
            similarTablesAnalysisBuilder.similarTables(candidateTableNames.stream().toList(), candidateTableNames.first());
            return candidateTableNames.size();
        }
        return 0;
    }

    private boolean isSimilarTables(List<Table> similarCandidates) {
        TableValidator tableValidator = ((SimilarTablesValidator.SimilarTablesValidatorBuilder) TableValidatorBuilderFactory.getTableValidatorBuilder(INFERENCE_RULE_NAME))
                .similarTables(similarCandidates)
                .build();
        if (!similarCandidates.isEmpty()) {
            int columnCount = Math.toIntExact(similarCandidates.get(0).stream()
                    .map(entry -> tableValidator.isValid(entry.getKey()))
                    .filter(value -> value)
                    .count());
            return (double) columnCount / similarCandidates.get(0).size() >= this.configuration.tableSimilarityThreshold();
        }
        return true;
    }

    private static class SimilarTablesInferenceRuleHolder {
        private static final SimilarTablesInferenceRule SIMILAR_TABLES_INFERENCE_RULE = new SimilarTablesInferenceRule();
    }
}
