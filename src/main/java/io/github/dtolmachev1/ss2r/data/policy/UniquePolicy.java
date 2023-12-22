package io.github.dtolmachev1.ss2r.data.policy;

import java.util.List;
import java.util.Map;

public interface UniquePolicy {
    <E> Map<Integer, E> resolve(Map<E, List<Integer>> values);
}
