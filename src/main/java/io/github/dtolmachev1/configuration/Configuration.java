package io.github.dtolmachev1.configuration;

import io.github.dtolmachev1.inference.policy.ColumnPolicy;
import io.github.dtolmachev1.inference.policy.TablePolicy;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.io.OutputStream;
import java.util.List;

public interface Configuration {
    void save(OutputStream outputStream);

    boolean mergeSimilarTables();

    double tableSimilarityThreshold();

    TablePolicy similarTablesPolicy();

    double typeThreshold();

    ColumnPolicy typePolicy();

    double uniqueThreshold();

    ColumnPolicy uniquePolicy();

    double referenceThreshold();

    ColumnPolicy referencePolicy();

    int multiValueReferenceLength();

    int multiValueReferenceCount();

    List<String> multiValueReferenceSeparators();

    double multiValueReferenceThreshold();

    ColumnPolicy multiValueReferencePolicy();

    DbmsConfiguration dbmsConfiguration();

    interface DbmsConfiguration {
        Element save(Document document);

        String driverClassName();

        String url();

        String username();

        String password();
    }
}
