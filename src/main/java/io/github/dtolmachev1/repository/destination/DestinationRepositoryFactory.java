package io.github.dtolmachev1.repository.destination;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

public class DestinationRepositoryFactory {
    private static final Map<String, Supplier<DestinationRepository>> DESTINATION_REPOSITORIES = Map.ofEntries(
            new AbstractMap.SimpleImmutableEntry<>(PostgresqlRepository.REPOSITORY_NAME, PostgresqlRepository::newInstance)
    );

    public static DestinationRepository getDestinationRepository(String name) {
        return Objects.requireNonNullElse(DESTINATION_REPOSITORIES.get(name), () -> null).get();
    }
}
