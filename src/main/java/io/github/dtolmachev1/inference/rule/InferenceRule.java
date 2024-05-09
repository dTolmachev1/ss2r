package io.github.dtolmachev1.inference.rule;

import io.github.dtolmachev1.data.database.Database;
import io.github.dtolmachev1.inference.analysis.Analysis;

import java.util.Optional;

public interface InferenceRule {
    String name();

    Optional<Analysis> apply(Database database);
}
