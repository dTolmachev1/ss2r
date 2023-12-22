package io.github.dtolmachev1.ss2r.data.policy;

import java.util.List;
import java.util.Map;

public class IgnoreReferencePolicy implements ReferencePolicy {
    private static final String POLICY_NAME = "ignore";

    private IgnoreReferencePolicy() {
    }

    public static IgnoreReferencePolicy newInstance() {
        return IgnoreReferencePolicyHolder.IGNORE_REFERENCE_POLICY;
    }

    @Override
    public <E> Map<Integer, E> resolve(Map<E, List<Integer>> values) {
        return Map.of();
    }

    public static String getName() {
        return POLICY_NAME;
    }

    private static class IgnoreReferencePolicyHolder {
        private static final IgnoreReferencePolicy IGNORE_REFERENCE_POLICY = new IgnoreReferencePolicy();
    }
}
