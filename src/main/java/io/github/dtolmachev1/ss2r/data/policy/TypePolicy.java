package io.github.dtolmachev1.ss2r.data.policy;

import java.util.Map;

public interface TypePolicy {
    <E> Map<Integer, E> resolve(Map<Integer, String> values);
}
