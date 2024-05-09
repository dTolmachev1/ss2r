package io.github.dtolmachev1.inference.analysis;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

public class AnalysisBuilderFactory {
    private static final Map<String, Supplier<Analysis.AnalysisBuilder>> ANALYSIS_BUILDERS = Map.ofEntries(
            new AbstractMap.SimpleImmutableEntry<>(ColumnNameAnalysis.ANALYSIS_NAME, ColumnNameAnalysis::builder),
            new AbstractMap.SimpleImmutableEntry<>(SimilarTablesAnalysis.ANALYSIS_NAME, SimilarTablesAnalysis::builder),
            new AbstractMap.SimpleImmutableEntry<>(TableNameAnalysis.ANALYSIS_NAME, TableNameAnalysis::builder),
            new AbstractMap.SimpleImmutableEntry<>(ColumnTypeAnalysis.ANALYSIS_NAME, ColumnTypeAnalysis::builder),
            new AbstractMap.SimpleImmutableEntry<>(UniqueConstraintAnalysis.ANALYSIS_NAME, UniqueConstraintAnalysis::builder),
            new AbstractMap.SimpleImmutableEntry<>(ReferenceConstraintAnalysis.ANALYSIS_NAME, ReferenceConstraintAnalysis::builder),
            new AbstractMap.SimpleImmutableEntry<>(MultiValueReferenceAnalysis.ANALYSIS_NAME, MultiValueReferenceAnalysis::builder)
    );

    public static Analysis.AnalysisBuilder getAnalysisBuilder(String name) {
        return Objects.requireNonNullElse(ANALYSIS_BUILDERS.get(name), () -> null).get();
    }
}
