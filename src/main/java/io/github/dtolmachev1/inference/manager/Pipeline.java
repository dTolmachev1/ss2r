package io.github.dtolmachev1.inference.manager;

import io.github.dtolmachev1.inference.rule.InferenceRule;

import java.util.stream.Stream;

public interface Pipeline extends Iterable<InferenceRule> {
    boolean isEmpty();

    int size();

    boolean contains(String inferenceRuleName);

    boolean register(InferenceRule inferenceRule);

    boolean deregister(String inferenceRuleName);

    Stream<InferenceRule> stream();

    Stream<InferenceRule> parallelStream();

    interface PipelineBuilder {
        PipelineBuilder register(InferenceRule inferenceRule);

        Pipeline build();
    }
}
