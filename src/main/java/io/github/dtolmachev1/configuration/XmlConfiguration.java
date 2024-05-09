package io.github.dtolmachev1.configuration;

import io.github.dtolmachev1.inference.policy.ColumnPolicy;
import io.github.dtolmachev1.inference.policy.ColumnPolicyFactory;
import io.github.dtolmachev1.inference.policy.IgnorePolicy;
import io.github.dtolmachev1.inference.policy.KeepFirstPolicy;
import io.github.dtolmachev1.inference.policy.RemovePolicy;
import io.github.dtolmachev1.inference.policy.TablePolicy;
import io.github.dtolmachev1.inference.policy.TablePolicyFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;

public class XmlConfiguration implements Configuration {
    @SuppressWarnings("HttpUrlsUsage")
    private static final String XML_INDENT_AMOUNT_KEY = "{http://xml.apache.org/xslt}indent-amount";
    private static final String CONFIGURATION_TAG = "config";
    private static final String MERGE_SIMILAR_TABLES_TAG = "merge-similar-tables";
    private static final String TABLE_SIMILARITY_THRESHOLD_TAG = "table-similarity-threshold";
    private static final String SIMILAR_TABLES_POLICY_TAG = "similar-tables-policy";
    private static final String TYPE_THRESHOLD_TAG = "type-threshold";
    private static final String TYPE_POLICY_TAG = "type-policy";
    private static final String UNIQUE_THRESHOLD_TAG = "unique-threshold";
    private static final String UNIQUE_POLICY_TAG = "unique-policy";
    private static final String REFERENCE_THRESHOLD_TAG = "reference-threshold";
    private static final String REFERENCE_POLICY_TAG = "reference-policy";
    private static final String MULTI_VALUE_REFERENCE_LENGTH_TAG = "multi-value-reference-length";
    private static final String MULTI_VALUE_REFERENCE_COUNT_TAG = "multi-value-reference-count";
    private static final String MULTI_VALUE_REFERENCE_SEPARATORS_TAG = "multi-value-reference-separators";
    private static final String MULTI_VALUE_REFERENCE_THRESHOLD_TAG = "multi-value-reference-threshold";
    private static final String MULTI_VALUE_REFERENCE_POLICY_TAG = "multi-value-reference-policy";
    private static final String DBMS_CONFIGURATION_TAG = "dbms-config";
    private static final boolean DEFAULT_MERGE_SIMILAR_TABLES = true;
    private static final double DEFAULT_TABLE_SIMILARITY_THRESHOLD = 0.8;
    private static final TablePolicy DEFAULT_SIMILAR_TABLES_POLICY = TablePolicyFactory.getTablePolicy(RemovePolicy.POLICY_NAME);
    private static final double DEFAULT_TYPE_THRESHOLD = 0.9;
    private static final ColumnPolicy DEFAULT_TYPE_POLICY = ColumnPolicyFactory.getColumnPolicy(IgnorePolicy.POLICY_NAME);
    private static final double DEFAULT_UNIQUE_THRESHOLD = 0.9;
    private static final ColumnPolicy DEFAULT_UNIQUE_POLICY = ColumnPolicyFactory.getColumnPolicy(IgnorePolicy.POLICY_NAME);
    private static final double DEFAULT_REFERENCE_THRESHOLD = 0.9;
    private static final ColumnPolicy DEFAULT_REFERENCE_POLICY = ColumnPolicyFactory.getColumnPolicy(IgnorePolicy.POLICY_NAME);
    private static final int DEFAULT_MULTI_VALUE_REFERENCE_LENGTH = 100;
    private static final int DEFAULT_MULTI_VALUE_REFERENCE_COUNT = 2;
    private static final List<String> DEFAULT_MULTI_VALUE_REFERENCE_SEPARATORS = List.of("|");
    private static final double DEFAULT_MULTI_VALUE_REFERENCE_THRESHOLD = 0.9;
    private static final ColumnPolicy DEFAULT_MULTI_VALUE_REFERENCE_POLICY = ColumnPolicyFactory.getColumnPolicy(IgnorePolicy.POLICY_NAME);
    private static final Set<String> SIMILAR_TABLES_POLICIES = Set.of(RemovePolicy.POLICY_NAME);
    private static final Set<String> TYPE_POLICIES = Set.of(IgnorePolicy.POLICY_NAME);
    private static final Set<String> UNIQUE_POLICIES = Set.of(IgnorePolicy.POLICY_NAME, KeepFirstPolicy.POLICY_NAME);
    private static final Set<String> REFERENCE_POLICIES = Set.of(IgnorePolicy.POLICY_NAME);
    private static final Set<String> MULTI_VALUE_REFERENCE_POLICIES = Set.of(IgnorePolicy.POLICY_NAME);
    private boolean mergeSimilarTables;
    private double tableSimilarityThreshold;
    private TablePolicy similarTablesPolicy;
    private double typeThreshold;
    private ColumnPolicy typePolicy;
    private double uniqueThreshold;
    private ColumnPolicy uniquePolicy;
    private double referenceThreshold;
    private ColumnPolicy referencePolicy;
    private int multiValueReferenceLength;
    private int multiValueReferenceCount;
    private List<String> multiValueReferenceSeparators;
    private double multiValueReferenceThreshold;
    private ColumnPolicy multiValueReferencePolicy;
    private DbmsConfiguration dbmsConfiguration;

    private XmlConfiguration() {
        this.mergeSimilarTables = DEFAULT_MERGE_SIMILAR_TABLES;
        this.tableSimilarityThreshold = DEFAULT_TABLE_SIMILARITY_THRESHOLD;
        this.similarTablesPolicy = DEFAULT_SIMILAR_TABLES_POLICY;
        this.typeThreshold = DEFAULT_TYPE_THRESHOLD;
        this.typePolicy = DEFAULT_TYPE_POLICY;
        this.uniqueThreshold = DEFAULT_UNIQUE_THRESHOLD;
        this.uniquePolicy = DEFAULT_UNIQUE_POLICY;
        this.referenceThreshold = DEFAULT_REFERENCE_THRESHOLD;
        this.referencePolicy = DEFAULT_REFERENCE_POLICY;
        this.multiValueReferenceLength = DEFAULT_MULTI_VALUE_REFERENCE_LENGTH;
        this.multiValueReferenceCount = DEFAULT_MULTI_VALUE_REFERENCE_COUNT;
        this.multiValueReferenceSeparators = DEFAULT_MULTI_VALUE_REFERENCE_SEPARATORS;
        this.multiValueReferenceThreshold = DEFAULT_MULTI_VALUE_REFERENCE_THRESHOLD;
        this.multiValueReferencePolicy = DEFAULT_MULTI_VALUE_REFERENCE_POLICY;
        this.dbmsConfiguration = XmlDbmsConfiguration.newInstance();
    }

    public static XmlConfiguration newInstance() {
        return XmlConfigurationHolder.XML_CONFIGURATION;
    }

    public static XmlConfiguration load(InputStream inputStream) {
        try {
            Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(inputStream);
            loadConfiguration(document.getDocumentElement().getElementsByTagName("*"));
            validate();
        } catch (ParserConfigurationException | SAXException | IOException e) {
            throw new RuntimeException("Unable to load configuration");
        }
        return newInstance();
    }

    @Override
    public void save(OutputStream outputStream) {
        try {
            Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
            document.appendChild(saveConfiguration(document));
            Transformer transformer = TransformerFactory.newInstance().newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.setOutputProperty(XML_INDENT_AMOUNT_KEY, "4");
            transformer.transform(new DOMSource(document), new StreamResult(outputStream));
        } catch (ParserConfigurationException | TransformerException e) {
            throw new RuntimeException("Unable to save configuration");
        }
    }

    @Override
    public boolean mergeSimilarTables() {
        return this.mergeSimilarTables;
    }

    @Override
    public double tableSimilarityThreshold() {
        return this.tableSimilarityThreshold;
    }

    @Override
    public TablePolicy similarTablesPolicy() {
        return this.similarTablesPolicy;
    }

    @Override
    public double typeThreshold() {
        return this.typeThreshold;
    }

    @Override
    public ColumnPolicy typePolicy() {
        return this.typePolicy;
    }

    @Override
    public double uniqueThreshold() {
        return this.uniqueThreshold;
    }

    @Override
    public ColumnPolicy uniquePolicy() {
        return this.uniquePolicy;
    }

    @Override
    public double referenceThreshold() {
        return this.referenceThreshold;
    }

    @Override
    public ColumnPolicy referencePolicy() {
        return this.referencePolicy;
    }

    @Override
    public int multiValueReferenceLength() {
        return this.multiValueReferenceLength;
    }

    @Override
    public int multiValueReferenceCount() {
        return this.multiValueReferenceCount;
    }

    @Override
    public List<String> multiValueReferenceSeparators() {
        return List.copyOf(this.multiValueReferenceSeparators);
    }

    @Override
    public double multiValueReferenceThreshold() {
        return this.multiValueReferenceThreshold;
    }

    @Override
    public ColumnPolicy multiValueReferencePolicy() {
        return this.multiValueReferencePolicy;
    }

    @Override
    public DbmsConfiguration dbmsConfiguration() {
        return this.dbmsConfiguration;
    }

    private static void loadConfiguration(NodeList configurationEntries) {
        if (configurationEntries.getLength() == 0) {
            throw new RuntimeException("Unable to load configuration");
        }
        for (int i = 0; i < configurationEntries.getLength(); i++) {
            if (!configurationEntries.item(i).getParentNode().getNodeName().equals(CONFIGURATION_TAG)) {
                continue;
            }
            Element configurationEntry = (Element) configurationEntries.item(i);
            if (!configurationEntry.getTagName().equals(DBMS_CONFIGURATION_TAG) && configurationEntry.getTextContent().isEmpty()) {
                throw new RuntimeException("Unable to load configuration");
            }
            switch (configurationEntry.getTagName()) {
                case MERGE_SIMILAR_TABLES_TAG -> XmlConfigurationHolder.XML_CONFIGURATION.mergeSimilarTables = Boolean.parseBoolean(configurationEntry.getTextContent());
                case TABLE_SIMILARITY_THRESHOLD_TAG -> XmlConfigurationHolder.XML_CONFIGURATION.tableSimilarityThreshold = Double.parseDouble(configurationEntry.getTextContent());
                case SIMILAR_TABLES_POLICY_TAG -> XmlConfigurationHolder.XML_CONFIGURATION.similarTablesPolicy = TablePolicyFactory.getTablePolicy(configurationEntry.getTextContent());
                case TYPE_THRESHOLD_TAG -> XmlConfigurationHolder.XML_CONFIGURATION.typeThreshold = Double.parseDouble(configurationEntry.getTextContent());
                case TYPE_POLICY_TAG -> XmlConfigurationHolder.XML_CONFIGURATION.typePolicy = ColumnPolicyFactory.getColumnPolicy(configurationEntry.getTextContent());
                case UNIQUE_THRESHOLD_TAG -> XmlConfigurationHolder.XML_CONFIGURATION.uniqueThreshold = Double.parseDouble(configurationEntry.getTextContent());
                case UNIQUE_POLICY_TAG -> XmlConfigurationHolder.XML_CONFIGURATION.uniquePolicy = ColumnPolicyFactory.getColumnPolicy(configurationEntry.getTextContent());
                case REFERENCE_THRESHOLD_TAG -> XmlConfigurationHolder.XML_CONFIGURATION.referenceThreshold = Double.parseDouble(configurationEntry.getTextContent());
                case REFERENCE_POLICY_TAG -> XmlConfigurationHolder.XML_CONFIGURATION.referencePolicy = ColumnPolicyFactory.getColumnPolicy(configurationEntry.getTextContent());
                case MULTI_VALUE_REFERENCE_LENGTH_TAG -> XmlConfigurationHolder.XML_CONFIGURATION.multiValueReferenceLength = Integer.parseInt(configurationEntry.getTextContent());
                case MULTI_VALUE_REFERENCE_COUNT_TAG -> XmlConfigurationHolder.XML_CONFIGURATION.multiValueReferenceCount = Integer.parseInt(configurationEntry.getTextContent());
                case MULTI_VALUE_REFERENCE_SEPARATORS_TAG -> XmlConfigurationHolder.XML_CONFIGURATION.multiValueReferenceSeparators = configurationEntry.getTextContent().chars().mapToObj(ch -> String.valueOf((char) ch)).toList();
                case MULTI_VALUE_REFERENCE_THRESHOLD_TAG -> XmlConfigurationHolder.XML_CONFIGURATION.multiValueReferenceThreshold = Double.parseDouble(configurationEntry.getTextContent());
                case MULTI_VALUE_REFERENCE_POLICY_TAG -> XmlConfigurationHolder.XML_CONFIGURATION.multiValueReferencePolicy = ColumnPolicyFactory.getColumnPolicy(configurationEntry.getTextContent());
                case DBMS_CONFIGURATION_TAG -> XmlConfigurationHolder.XML_CONFIGURATION.dbmsConfiguration = XmlDbmsConfiguration.load(configurationEntry.getElementsByTagName("*"));
            }
        }
    }

    private Element saveConfiguration(Document document) {
        Element configurationNode = document.createElement(CONFIGURATION_TAG);
        Element mergeSimilarTablesNode = document.createElement(MERGE_SIMILAR_TABLES_TAG);
        mergeSimilarTablesNode.setTextContent(Boolean.toString(this.mergeSimilarTables));
        configurationNode.appendChild(mergeSimilarTablesNode);
        Element tableSimilarityThresholdNode = document.createElement(TABLE_SIMILARITY_THRESHOLD_TAG);
        tableSimilarityThresholdNode.setTextContent(Double.toString(this.tableSimilarityThreshold));
        configurationNode.appendChild(tableSimilarityThresholdNode);
        Element similarTablesPolicyNode = document.createElement(SIMILAR_TABLES_POLICY_TAG);
        similarTablesPolicyNode.setTextContent(this.similarTablesPolicy.name());
        configurationNode.appendChild(similarTablesPolicyNode);
        Element typeThresholdNode = document.createElement(TYPE_THRESHOLD_TAG);
        typeThresholdNode.setTextContent(Double.toString(this.typeThreshold));
        configurationNode.appendChild(typeThresholdNode);
        Element typePolicyNode = document.createElement(TYPE_POLICY_TAG);
        typePolicyNode.setTextContent(this.typePolicy.name());
        configurationNode.appendChild(typePolicyNode);
        Element uniqueThresholdNode = document.createElement(UNIQUE_THRESHOLD_TAG);
        uniqueThresholdNode.setTextContent(Double.toString(this.uniqueThreshold));
        configurationNode.appendChild(uniqueThresholdNode);
        Element uniquePolicyNode = document.createElement(UNIQUE_POLICY_TAG);
        uniquePolicyNode.setTextContent(this.uniquePolicy.name());
        configurationNode.appendChild(uniquePolicyNode);
        Element referenceThresholdNode = document.createElement(REFERENCE_THRESHOLD_TAG);
        referenceThresholdNode.setTextContent(Double.toString(this.referenceThreshold));
        configurationNode.appendChild(referenceThresholdNode);
        Element referencePolicyNode = document.createElement(REFERENCE_POLICY_TAG);
        referencePolicyNode.setTextContent(this.referencePolicy.name());
        configurationNode.appendChild(referencePolicyNode);
        Element multiValueReferenceLengthNode = document.createElement(MULTI_VALUE_REFERENCE_LENGTH_TAG);
        multiValueReferenceLengthNode.setTextContent(Integer.toString(this.multiValueReferenceLength));
        configurationNode.appendChild(multiValueReferenceLengthNode);
        Element multiValueReferenceCountNode = document.createElement(MULTI_VALUE_REFERENCE_COUNT_TAG);
        multiValueReferenceCountNode.setTextContent(Integer.toString(this.multiValueReferenceCount));
        configurationNode.appendChild(multiValueReferenceCountNode);
        Element multiValueReferenceSeparatorsNode = document.createElement(MULTI_VALUE_REFERENCE_SEPARATORS_TAG);
        multiValueReferenceSeparatorsNode.setTextContent(String.join("", this.multiValueReferenceSeparators));
        configurationNode.appendChild(multiValueReferenceSeparatorsNode);
        Element multiValueReferenceThresholdNode = document.createElement(MULTI_VALUE_REFERENCE_THRESHOLD_TAG);
        multiValueReferenceThresholdNode.setTextContent(Double.toString(this.multiValueReferenceThreshold));
        configurationNode.appendChild(multiValueReferenceThresholdNode);
        Element multiValueReferencePolicyNode = document.createElement(MULTI_VALUE_REFERENCE_POLICY_TAG);
        multiValueReferencePolicyNode.setTextContent(this.multiValueReferencePolicy.name());
        configurationNode.appendChild(multiValueReferencePolicyNode);
        configurationNode.appendChild(dbmsConfiguration.save(document));
        return configurationNode;
    }

    private static void validate() throws RuntimeException {
        boolean validSimilarTablesPolicy = SIMILAR_TABLES_POLICIES.contains(XmlConfigurationHolder.XML_CONFIGURATION.similarTablesPolicy.name());
        boolean validTypePolicy = TYPE_POLICIES.contains(XmlConfigurationHolder.XML_CONFIGURATION.typePolicy.name());
        boolean validUniquePolicy = UNIQUE_POLICIES.contains(XmlConfigurationHolder.XML_CONFIGURATION.uniquePolicy.name());
        boolean validReferencePolicy = REFERENCE_POLICIES.contains(XmlConfigurationHolder.XML_CONFIGURATION.referencePolicy.name());
        boolean validMultiValueReferencePolicy = MULTI_VALUE_REFERENCE_POLICIES.contains(XmlConfigurationHolder.XML_CONFIGURATION.multiValueReferencePolicy.name());
        if (!validSimilarTablesPolicy || !validTypePolicy || !validUniquePolicy || !validReferencePolicy || !validMultiValueReferencePolicy) {
            throw new RuntimeException("Invalid configuration");
        }
    }

    public static class XmlDbmsConfiguration implements Configuration.DbmsConfiguration {
        private static final String DRIVER_CLASS_NAME_TAG = "driver-class-name";
        private static final String URL_TAG = "url";
        private static final String USERNAME_TAG = "username";
        private static final String PASSWORD_TAG = "password";
        private static final String DEFAULT_DRIVER_CLASS_NAME = "org.postgresql.Driver";
        private static final String DEFAULT_URL = "jdbc:postgresql://localhost:5432";
        private static final String DEFAULT_USERNAME = "user";
        private static final String DEFAULT_PASSWORD = "password";
        private String driverClassName;
        private String url;
        private String username;
        private String password;

        private XmlDbmsConfiguration() {
            this.driverClassName = DEFAULT_DRIVER_CLASS_NAME;
            this.url = DEFAULT_URL;
            this.username = DEFAULT_USERNAME;
            this.password = DEFAULT_PASSWORD;
        }

        public static XmlDbmsConfiguration newInstance() {
            return XmlDbmsConfigurationHolder.XML_DBMS_CONFIGURATION;
        }

        public static XmlDbmsConfiguration load(NodeList configurationEntries) {
            loadConfiguration(configurationEntries);
            return newInstance();
        }

        @Override
        public Element save(Document document) {
            return saveConfiguration(document);
        }

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

        private static void loadConfiguration(NodeList configurationEntries) {
            if (configurationEntries.getLength() == 0) {
                throw new RuntimeException("Unable to load dbms-configuration");
            }
            for (int i = 0; i < configurationEntries.getLength(); i++) {
                if (!configurationEntries.item(i).getParentNode().getNodeName().equals(DBMS_CONFIGURATION_TAG)) {
                    continue;
                }
                Element configurationEntry = (Element) configurationEntries.item(i);
                if (configurationEntry.getTextContent().isEmpty()) {
                    throw new RuntimeException("Unable to load dbms-configuration");
                }
                switch (configurationEntry.getNodeName()) {
                    case DRIVER_CLASS_NAME_TAG -> XmlDbmsConfigurationHolder.XML_DBMS_CONFIGURATION.driverClassName = configurationEntry.getTextContent();
                    case URL_TAG -> XmlDbmsConfigurationHolder.XML_DBMS_CONFIGURATION.url = configurationEntry.getTextContent();
                    case USERNAME_TAG -> XmlDbmsConfigurationHolder.XML_DBMS_CONFIGURATION.username = configurationEntry.getTextContent();
                    case PASSWORD_TAG -> XmlDbmsConfigurationHolder.XML_DBMS_CONFIGURATION.password = configurationEntry.getTextContent();
                }
            }
        }

        private Element saveConfiguration(Document document) {
            Element configurationNode = document.createElement(DBMS_CONFIGURATION_TAG);
            Element driverClassNameNode = document.createElement(DRIVER_CLASS_NAME_TAG);
            driverClassNameNode.setTextContent(this.driverClassName);
            configurationNode.appendChild(driverClassNameNode);
            Element urlNode = document.createElement(URL_TAG);
            urlNode.setTextContent(this.url);
            configurationNode.appendChild(urlNode);
            Element usernameNode = document.createElement(USERNAME_TAG);
            usernameNode.setTextContent(this.username);
            configurationNode.appendChild(usernameNode);
            Element passwordNode = document.createElement(PASSWORD_TAG);
            passwordNode.setTextContent(this.password);
            configurationNode.appendChild(passwordNode);
            return configurationNode;
        }

        private static class XmlDbmsConfigurationHolder {
            private static final XmlDbmsConfiguration XML_DBMS_CONFIGURATION = new XmlDbmsConfiguration();
        }
    }

    private static class XmlConfigurationHolder {
        private static final XmlConfiguration XML_CONFIGURATION = new XmlConfiguration();
    }
}
