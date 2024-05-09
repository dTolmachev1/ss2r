package io.github.dtolmachev1.repository.source;

import io.github.dtolmachev1.data.database.Database;

import java.nio.file.Path;

public interface SourceRepository {
    Database load(Path path, String databaseName);
}
