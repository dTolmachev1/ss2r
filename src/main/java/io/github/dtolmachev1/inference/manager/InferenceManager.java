package io.github.dtolmachev1.inference.manager;

import io.github.dtolmachev1.data.database.Database;

import java.io.InputStream;
import java.io.OutputStream;

public interface InferenceManager {
    void loadAnalyzes(InputStream inputStream, Database database);

    void saveAnalyzes(OutputStream outputStream);

    void analyzeDatabase(Database database);
}
