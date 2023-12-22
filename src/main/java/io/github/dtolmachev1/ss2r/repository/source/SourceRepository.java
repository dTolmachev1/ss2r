package io.github.dtolmachev1.ss2r.repository.source;

import io.github.dtolmachev1.ss2r.data.database.Database;

public interface SourceRepository {
    Database load();
}
