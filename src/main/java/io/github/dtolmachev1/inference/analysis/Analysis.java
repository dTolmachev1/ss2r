package io.github.dtolmachev1.inference.analysis;

import io.github.dtolmachev1.data.database.Database;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public interface Analysis {
    String name();

    Element save(Document document);

    void transform(Database database);

    interface AnalysisBuilder {
        Analysis build();
    }
}
