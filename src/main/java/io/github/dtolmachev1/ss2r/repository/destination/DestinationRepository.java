package io.github.dtolmachev1.ss2r.repository.destination;

import io.github.dtolmachev1.ss2r.data.database.Database;

public interface DestinationRepository {
    void save(Database database);
}
