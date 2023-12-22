package io.github.dtolmachev1.ss2r.data.policy;

import java.util.List;
import java.util.Map;

public class IgnoreUniquePolicy implements UniquePolicy {
    private static final String POLICY_NAME = "ignore";
    private IgnoreUniquePolicy() {
    }

    public static IgnoreUniquePolicy newInstance() {
        return IgnoreUniquePolicyHolder.IGNORE_UNIQUE_POLICY;
    }

    @Override
    public <E> Map<Integer, E> resolve(Map<E, List<Integer>> values) {
        return Map.of();
    }

    public static String getName() {
        return POLICY_NAME;
    }

    private static class IgnoreUniquePolicyHolder {
        private static final IgnoreUniquePolicy IGNORE_UNIQUE_POLICY = new IgnoreUniquePolicy();
    }
}
