package io.github.dtolmachev1.ss2r.configuration;

import io.github.dtolmachev1.ss2r.data.policy.ReferencePolicy;
import io.github.dtolmachev1.ss2r.data.policy.TypePolicy;
import io.github.dtolmachev1.ss2r.data.policy.UniquePolicy;
import jakarta.xml.bind.JAXBException;

import java.io.OutputStream;

public interface Configuration {
    void save(OutputStream outputStream) throws JAXBException;

    boolean mergeSimilarTables();

    double typeThreshold();

    TypePolicy typePolicy();

    double uniqueThreshold();

    UniquePolicy uniquePolicy();

    double referenceThreshold();

    ReferencePolicy referencePolicy();

    DbmsConfiguration dbmsConfiguration();

    interface DbmsConfiguration {
        String driverClassName();

        String url();

        String username();

        String password();
    }
}
