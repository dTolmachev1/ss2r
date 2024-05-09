package io.github.dtolmachev1.inference.rule;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

public class InferenceRuleFactory {
    private static final Map<String, Supplier<InferenceRule>> INFERENCE_RULES = Map.ofEntries(
            new AbstractMap.SimpleImmutableEntry<>(ColumnNameInferenceRule.INFERENCE_RULE_NAME, ColumnNameInferenceRule::newInstance),
            new AbstractMap.SimpleImmutableEntry<>(SimilarTablesInferenceRule.INFERENCE_RULE_NAME, SimilarTablesInferenceRule::newInstance),
            new AbstractMap.SimpleImmutableEntry<>(TableNameInferenceRule.INFERENCE_RULE_NAME, TableNameInferenceRule::newInstance),
            new AbstractMap.SimpleImmutableEntry<>(ColumnTypeInferenceRule.INFERENCE_RULE_NAME, ColumnTypeInferenceRule::newInstance),
            new AbstractMap.SimpleImmutableEntry<>(UniqueConstraintInferenceRule.INFERENCE_RULE_NAME, UniqueConstraintInferenceRule::newInstance),
            new AbstractMap.SimpleImmutableEntry<>(ReferenceConstraintInferenceRule.INFERENCE_RULE_NAME, ReferenceConstraintInferenceRule::newInstance),
            new AbstractMap.SimpleImmutableEntry<>(MultiValueReferenceInferenceRule.INFERENCE_RULE_NAME, MultiValueReferenceInferenceRule::newInstance)
    );

    public static InferenceRule getInferenceRule(String name) {
        return Objects.requireNonNullElse(INFERENCE_RULES.get(name), () -> null).get();
    }
}
