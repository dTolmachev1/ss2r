package io.github.dtolmachev1.repository.destination;

import io.github.dtolmachev1.data.database.Database;

public interface DestinationRepository {
    void save(Database database);
}
