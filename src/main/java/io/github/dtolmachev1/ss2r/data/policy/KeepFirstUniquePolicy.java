package io.github.dtolmachev1.ss2r.data.policy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

public class KeepFirstUniquePolicy implements UniquePolicy {
    private static final String POLICY_NAME = "keep first";
    private KeepFirstUniquePolicy() {
    }

    public static KeepFirstUniquePolicy newInstance() {
        return KeepFirstUniquePolicyHolder.KEEP_FIRST_UNIQUE_POLICY;
    }

    @Override
    public <E> Map<Integer, E> resolve(Map<E, List<Integer>> values) {
        Map<Integer, E> result = new HashMap<>();
        Optional<Entry<E, List<Integer>>> entry = values.entrySet().stream().findAny();
        if (entry.isPresent() && !entry.get().getValue().isEmpty()) {
            result.put(entry.get().getValue().get(0), entry.get().getKey());
        }
        return result;
    }

    public static String getName() {
        return POLICY_NAME;
    }

    private static class KeepFirstUniquePolicyHolder {
        private static final KeepFirstUniquePolicy KEEP_FIRST_UNIQUE_POLICY = new KeepFirstUniquePolicy();
    }
}
