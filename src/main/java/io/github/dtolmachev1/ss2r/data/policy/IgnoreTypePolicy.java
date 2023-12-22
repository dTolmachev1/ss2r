package io.github.dtolmachev1.ss2r.data.policy;

import java.util.Map;

public class IgnoreTypePolicy implements TypePolicy {
    private static final String POLICY_NAME = "ignore";
    private IgnoreTypePolicy() {
    }

    public static IgnoreTypePolicy newInstance() {
        return IgnoreTypePolicyHolder.IGNORE_TYPE_POLICY;
    }

    @Override
    public <E> Map<Integer, E> resolve                      (Map<Integer, String> values) {
        return Map.of();
    }

    public static String getName() {
        return POLICY_NAME;
    }

    private static class IgnoreTypePolicyHolder {
        private static final IgnoreTypePolicy IGNORE_TYPE_POLICY = new IgnoreTypePolicy();
    }
}
