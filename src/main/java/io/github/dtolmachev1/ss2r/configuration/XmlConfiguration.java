package io.github.dtolmachev1.ss2r.configuration;

import io.github.dtolmachev1.ss2r.data.policy.IgnoreReferencePolicy;
import io.github.dtolmachev1.ss2r.data.policy.IgnoreTypePolicy;
import io.github.dtolmachev1.ss2r.data.policy.IgnoreUniquePolicy;
import io.github.dtolmachev1.ss2r.data.policy.KeepFirstUniquePolicy;
import io.github.dtolmachev1.ss2r.data.policy.ReferencePolicy;
import io.github.dtolmachev1.ss2r.data.policy.TypePolicy;
import io.github.dtolmachev1.ss2r.data.policy.UniquePolicy;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Marshaller;
import jakarta.xml.bind.Unmarshaller;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map;

@XmlRootElement(name = "config")
@XmlAccessorType(XmlAccessType.FIELD)
public class XmlConfiguration implements Configuration {
    private static final Map<String, TypePolicy> TYPE_POLICIES = Map.ofEntries(
            new SimpleImmutableEntry<>(IgnoreTypePolicy.getName(), IgnoreTypePolicy.newInstance())
    );
    private static final Map<String, UniquePolicy> UNIQUE_POLICIES = Map.ofEntries(
            new SimpleImmutableEntry<>(IgnoreUniquePolicy.getName(), IgnoreUniquePolicy.newInstance()),
            new SimpleImmutableEntry<>(KeepFirstUniquePolicy.getName(), KeepFirstUniquePolicy.newInstance())
    );
    private static final Map<String, ReferencePolicy> REFERENCE_POLICIES = Map.ofEntries(
            new SimpleImmutableEntry<>(IgnoreReferencePolicy.getName(), IgnoreReferencePolicy.newInstance())
    );
    @XmlElement(name = "merge-similar-tables")
    private boolean mergeSimilarTables;
    @XmlElement(name = "type-threshold")
    private double typeThreshold;
    @XmlElement(name = "type-policy")
    private String typePolicy;
    @XmlElement(name = "unique-threshold")
    private double uniqueThreshold;
    @XmlElement(name = "unique-policy")
    private String uniquePolicy;
    @XmlElement(name = "reference-threshold")
    private double referenceThreshold;
    @XmlElement(name = "reference-policy")
    private String referencePolicy;
    @XmlElement(name = "dbms-config")
    private XmlDbmsConfiguration dbmsConfiguration;

    public XmlConfiguration() {
        this.mergeSimilarTables = true;
        this.typeThreshold = 0.95;
        this.typePolicy = IgnoreTypePolicy.getName();
        this.uniqueThreshold = 0.95;
        this.uniquePolicy = KeepFirstUniquePolicy.getName();
        this.referenceThreshold = 0.95;
        this.referencePolicy = IgnoreReferencePolicy.getName();
        this.dbmsConfiguration = new XmlDbmsConfiguration();
    }

    public static XmlConfiguration newInstance() {
        return XmlConfigurationHolder.XML_CONFIGURATION;
    }

    public static XmlConfiguration newInstance(InputStream inputStream) {
        try {
            JAXBContext context = JAXBContext.newInstance(XmlConfiguration.class);
            Unmarshaller reader = context.createUnmarshaller();
            XmlConfiguration configuration = (XmlConfiguration) reader.unmarshal(inputStream);
            XmlConfigurationHolder.XML_CONFIGURATION.mergeSimilarTables = configuration.mergeSimilarTables;
            XmlConfigurationHolder.XML_CONFIGURATION.typeThreshold = configuration.typeThreshold;
            XmlConfigurationHolder.XML_CONFIGURATION.typePolicy = configuration.typePolicy;
            XmlConfigurationHolder.XML_CONFIGURATION.uniqueThreshold = configuration.uniqueThreshold;
            XmlConfigurationHolder.XML_CONFIGURATION.uniquePolicy = configuration.uniquePolicy;
            XmlConfigurationHolder.XML_CONFIGURATION.referenceThreshold = configuration.referenceThreshold;
            XmlConfigurationHolder.XML_CONFIGURATION.referencePolicy = configuration.referencePolicy;
            XmlConfigurationHolder.XML_CONFIGURATION.dbmsConfiguration = configuration.dbmsConfiguration;
            validate();
        } catch (JAXBException e) {
            throw new RuntimeException("Unable to parse configuration");
        }
        return XmlConfigurationHolder.XML_CONFIGURATION;
    }

    @Override
    public void save(OutputStream outputStream) {
        try {
            JAXBContext context = JAXBContext.newInstance(XmlConfiguration.class);
            Marshaller writer = context.createMarshaller();
            writer.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
            writer.marshal(this, outputStream);
        } catch (JAXBException e) {
            throw new RuntimeException("Unable to save configuration");
        }
    }

    @Override
    public boolean mergeSimilarTables() {
        return this.mergeSimilarTables;
    }

    @Override
    public double typeThreshold() {
        return this.typeThreshold;
    }

    @Override
    public TypePolicy typePolicy() {
        return TYPE_POLICIES.get(this.typePolicy);
    }

    @Override
    public double uniqueThreshold() {
        return this.uniqueThreshold;
    }

    @Override
    public UniquePolicy uniquePolicy() {
        return UNIQUE_POLICIES.get(this.uniquePolicy);
    }

    @Override
    public double referenceThreshold() {
        return this.referenceThreshold;
    }

    @Override
    public ReferencePolicy referencePolicy() {
        return REFERENCE_POLICIES.get(this.referencePolicy);
    }

    @Override
    public XmlDbmsConfiguration dbmsConfiguration() {
        return this.dbmsConfiguration;
    }

    public boolean isMergeSimilarTables() {
        return this.mergeSimilarTables;
    }

    public void setMergeSimilarTables(boolean mergeSimilarTables) {
        this.mergeSimilarTables = mergeSimilarTables;
    }

    public double getTypeThreshold() {
        return this.typeThreshold;
    }

    public void setTypeThreshold(double typeThreshold) {
        this.typeThreshold = typeThreshold;
    }

    public String getTypePolicy() {
        return this.typePolicy;
    }

    public void setTypePolicy(String typePolicy) {
        this.typePolicy = typePolicy;
    }

    public double getUniqueThreshold() {
        return this.uniqueThreshold;
    }

    public void setUniqueThreshold(double uniqueThreshold) {
        this.uniqueThreshold = uniqueThreshold;
    }

    public String getUniquePolicy() {
        return this.uniquePolicy;
    }

    public void setUniquePolicy(String uniquePolicy) {
        this.uniquePolicy = uniquePolicy;
    }

    public double getReferenceThreshold() {
        return this.referenceThreshold;
    }

    public void setReferenceThreshold(double referenceThreshold) {
        this.referenceThreshold = referenceThreshold;
    }

    public String getReferencePolicy() {
        return this.referencePolicy;
    }

    public void setReferencePolicy(String referencePolicy) {
        this.referencePolicy = referencePolicy;
    }

    public XmlDbmsConfiguration getDbmsConfiguration() {
        return this.dbmsConfiguration;
    }

    public void setDbmsConfiguration(XmlDbmsConfiguration dbmsConfiguration) {
        this.dbmsConfiguration = dbmsConfiguration;
    }

    private static void validate() throws RuntimeException {
        if (!TYPE_POLICIES.containsKey(XmlConfigurationHolder.XML_CONFIGURATION.typePolicy) || !UNIQUE_POLICIES.containsKey(XmlConfigurationHolder.XML_CONFIGURATION.uniquePolicy) || !REFERENCE_POLICIES.containsKey(XmlConfigurationHolder.XML_CONFIGURATION.referencePolicy)) {
            throw new RuntimeException("Invalid configuration");
        }
    }

    @XmlAccessorType(XmlAccessType.FIELD)
    public static class XmlDbmsConfiguration implements DbmsConfiguration {
        @XmlElement(name = "driver-class-name", required = true)
        private String driverClassName;
        @XmlElement(name = "url", required = true)
        private String url;
        @XmlElement(name = "username", required = true)
        private String username;
        @XmlElement(name = "password", required = true)
        private String password;

        @Override
        public String driverClassName() {
            return this.driverClassName;
        }

        @Override
        public String url() {
            return this.url;
        }

        @Override
        public String username() {
            return this.username;
        }

        @Override
        public String password() {
            return this.password;
        }

        public String getDriverClassName() {
            return this.driverClassName;
        }

        public void setDriverClassName(String driverClassName) {
            this.driverClassName = driverClassName;
        }

        public String getUrl() {
            return this.url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getUsername() {
            return this.username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return this.password;
        }

        public void setPassword(String password) {
            this.password = password;
        }
    }

    private static class XmlConfigurationHolder {
        private static final XmlConfiguration XML_CONFIGURATION = new XmlConfiguration();
    }
}
