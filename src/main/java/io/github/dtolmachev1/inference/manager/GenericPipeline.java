package io.github.dtolmachev1.inference.manager;

import io.github.dtolmachev1.inference.rule.ColumnNameInferenceRule;
import io.github.dtolmachev1.inference.rule.ColumnTypeInferenceRule;
import io.github.dtolmachev1.inference.rule.InferenceRule;
import io.github.dtolmachev1.inference.rule.InferenceRuleFactory;
import io.github.dtolmachev1.inference.rule.MultiValueReferenceInferenceRule;
import io.github.dtolmachev1.inference.rule.ReferenceConstraintInferenceRule;
import io.github.dtolmachev1.inference.rule.SimilarTablesInferenceRule;
import io.github.dtolmachev1.inference.rule.TableNameInferenceRule;
import io.github.dtolmachev1.inference.rule.UniqueConstraintInferenceRule;
import jakarta.annotation.Nonnull;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class GenericPipeline implements Pipeline {
    private final Map<String, InferenceRule> data;

    public GenericPipeline() {
        this.data = new LinkedHashMap<>();
    }

    public static GenericPipeline defaultPipeline() {
        return builder()
                .register(InferenceRuleFactory.getInferenceRule(ColumnNameInferenceRule.INFERENCE_RULE_NAME))
                .register(InferenceRuleFactory.getInferenceRule(SimilarTablesInferenceRule.INFERENCE_RULE_NAME))
                .register(InferenceRuleFactory.getInferenceRule(TableNameInferenceRule.INFERENCE_RULE_NAME))
                .register(InferenceRuleFactory.getInferenceRule(ColumnTypeInferenceRule.INFERENCE_RULE_NAME))
                .register(InferenceRuleFactory.getInferenceRule(UniqueConstraintInferenceRule.INFERENCE_RULE_NAME))
                .register(InferenceRuleFactory.getInferenceRule(ReferenceConstraintInferenceRule.INFERENCE_RULE_NAME))
                .register(InferenceRuleFactory.getInferenceRule(MultiValueReferenceInferenceRule.INFERENCE_RULE_NAME))
                .build();
    }

    @Override
    public boolean isEmpty() {
        return this.data.isEmpty();
    }

    @Override
    public int size() {
        return this.data.size();
    }

    @Override
    public boolean contains(String inferenceRuleName) {
        return this.data.containsKey(inferenceRuleName);
    }

    @Override
    public boolean register(InferenceRule inferenceRule) {
        return Objects.isNull(this.data.putIfAbsent(inferenceRule.name(), inferenceRule));
    }

    @Override
    public boolean deregister(String inferenceRuleName) {
        return Objects.nonNull(this.data.remove(inferenceRuleName));
    }

    public static GenericPipelineBuilder builder() {
        return new GenericPipelineBuilder();
    }

    @Override
    public Stream<InferenceRule> stream() {
        return this.data.values().stream();
    }

    @Override
    public Stream<InferenceRule> parallelStream() {
        return this.data.values().parallelStream();
    }

    @Override
    @Nonnull
    public Iterator<InferenceRule> iterator() {
        return this.data.values().iterator();
    }

    @Override
    public void forEach(Consumer<? super InferenceRule> action) {
        this.data.values().forEach(action);
    }

    @Override
    public Spliterator<InferenceRule> spliterator() {
        return this.data.values().spliterator();
    }

    public static class GenericPipelineBuilder implements PipelineBuilder {
        private final GenericPipeline pipeline;

        private GenericPipelineBuilder() {
            this.pipeline = new GenericPipeline();
        }

        @Override
        public GenericPipelineBuilder register(InferenceRule inferenceRule) {
            this.pipeline.register(inferenceRule);
            return this;
        }

        @Override
        public GenericPipeline build() {
            return this.pipeline;
        }
    }
}
