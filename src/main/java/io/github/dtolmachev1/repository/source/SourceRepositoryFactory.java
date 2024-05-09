package io.github.dtolmachev1.repository.source;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

public class SourceRepositoryFactory {
    private static final Map<String, Supplier<SourceRepository>> SOURCE_REPOSITORIES = Map.ofEntries(
            new AbstractMap.SimpleImmutableEntry<>(CsvRepository.REPOSITORY_NAME, CsvRepository::newInstance)
    );

    public static SourceRepository getSourceRepository(String name) {
        return Objects.requireNonNullElse(SOURCE_REPOSITORIES.get(name), () -> null).get();
    }
}
