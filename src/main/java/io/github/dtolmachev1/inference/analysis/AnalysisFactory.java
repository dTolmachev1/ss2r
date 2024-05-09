package io.github.dtolmachev1.inference.analysis;

import org.w3c.dom.NodeList;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class AnalysisFactory {
    private static final Map<String, Function<NodeList, Analysis>> ANALYZES = Map.ofEntries(
            new AbstractMap.SimpleImmutableEntry<>(ColumnNameAnalysis.ANALYSIS_NAME, ColumnNameAnalysis::load),
            new AbstractMap.SimpleImmutableEntry<>(SimilarTablesAnalysis.ANALYSIS_NAME, SimilarTablesAnalysis::load),
            new AbstractMap.SimpleImmutableEntry<>(TableNameAnalysis.ANALYSIS_NAME, TableNameAnalysis::load),
            new AbstractMap.SimpleImmutableEntry<>(ColumnTypeAnalysis.ANALYSIS_NAME, ColumnTypeAnalysis::load),
            new AbstractMap.SimpleImmutableEntry<>(UniqueConstraintAnalysis.ANALYSIS_NAME, UniqueConstraintAnalysis::load),
            new AbstractMap.SimpleImmutableEntry<>(ReferenceConstraintAnalysis.ANALYSIS_NAME, ReferenceConstraintAnalysis::load),
            new AbstractMap.SimpleImmutableEntry<>(MultiValueReferenceAnalysis.ANALYSIS_NAME, MultiValueReferenceAnalysis::load)
    );

    public static Analysis loadAnalysis(String name, NodeList analysisEntries) {
        return Objects.requireNonNullElse(ANALYZES.get(name), arg -> null).apply(analysisEntries);
    }
}
