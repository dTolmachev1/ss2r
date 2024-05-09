package io.github.dtolmachev1.inference.analysis;

import io.github.dtolmachev1.configuration.Configuration;
import io.github.dtolmachev1.configuration.XmlConfiguration;
import io.github.dtolmachev1.data.column.Column;
import io.github.dtolmachev1.data.database.Database;
import io.github.dtolmachev1.data.table.Constraint;
import io.github.dtolmachev1.data.table.ConstraintBuilderFactory;
import io.github.dtolmachev1.data.table.ReferenceConstraint;
import io.github.dtolmachev1.data.table.Table;
import io.github.dtolmachev1.inference.validator.ReferenceConstraintValidator;
import io.github.dtolmachev1.inference.validator.ColumnValidator;
import io.github.dtolmachev1.inference.validator.ColumnValidatorBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ReferenceConstraintAnalysis implements Analysis {
    public static final String ANALYSIS_NAME = "reference-constraint";
    private static final String ANALYSIS_TAG = "analysis";
    private static final String ANALYSIS_NAME_TAG = "analysis-name";
    private static final String REFERENCING_TABLE_TAG = "referencing-table";
    private static final String REFERENCING_COLUMN_TAG = "referencing-column";
    private static final String REFERENCED_TABLE_TAG = "referenced-table";
    private static final String REFERENCED_COLUMN_TAG = "referenced-column";
    private final Map<String, Map<String, Map.Entry<String, String>>> referenceConstraints;
    private final Configuration configuration;

    private ReferenceConstraintAnalysis() {
        this.referenceConstraints = new HashMap<>();
        this.configuration = XmlConfiguration.newInstance();
    }

    @Override
    public String name() {
        return ANALYSIS_NAME;
    }

    public static ReferenceConstraintAnalysis load(NodeList referenceConstraintEntries) {
        if (referenceConstraintEntries.getLength() == 0) {
            throw new RuntimeException("Unable to load analyzes");
        }
        ReferenceConstraintAnalysisBuilder referenceConstraintAnalysisBuilder = builder();
        for (int i = 0; i < referenceConstraintEntries.getLength(); i++) {
            loadAnalysis((Element) referenceConstraintEntries.item(i), referenceConstraintAnalysisBuilder);
        }
        return referenceConstraintAnalysisBuilder.build();
    }

    @Override
    public Element save(Document document) {
        Element analysisNode = document.createElement(ANALYSIS_TAG);
        Element analysisNameNode = document.createElement(ANALYSIS_NAME_TAG);
        analysisNameNode.setTextContent(ANALYSIS_NAME);
        analysisNode.appendChild(analysisNameNode);
        this.referenceConstraints.forEach((referencingTableName, referencingTableEntry) -> referencingTableEntry.forEach((referencingColumnName, referencingColumnEntry) -> analysisNode.appendChild(saveAnalysis(referencingTableName, referencingColumnName, referencingColumnEntry.getKey(), referencingColumnEntry.getValue(), document))));
        return analysisNode;
    }

    @Override
    public void transform(Database database) {
        this.referenceConstraints.forEach((referencingTableName, referencingTableEntry) -> transformTable(referencingTableEntry, database, database.get(referencingTableName)));
    }

    public static ReferenceConstraintAnalysisBuilder builder() {
        return new ReferenceConstraintAnalysisBuilder();
    }

    @SuppressWarnings("DuplicatedCode")
    private static void loadAnalysis(Element referenceConstraintEntry, ReferenceConstraintAnalysisBuilder referenceConstraintAnalysisBuilder) {
        if (!referenceConstraintEntry.hasChildNodes()) {
            throw new RuntimeException("Unable to load analyzes");
        }
        String referencingTableName = referenceConstraintEntry.getElementsByTagName(REFERENCING_TABLE_TAG).item(0).getTextContent();
        String referencingColumnName = referenceConstraintEntry.getElementsByTagName(REFERENCING_COLUMN_TAG).item(0).getTextContent();
        String referencedTableName = referenceConstraintEntry.getElementsByTagName(REFERENCED_TABLE_TAG).item(0).getTextContent();
        String referencedColumnName = referenceConstraintEntry.getElementsByTagName(REFERENCED_COLUMN_TAG).item(0).getTextContent();
        referenceConstraintAnalysisBuilder.referenceConstraint(referencingTableName, referencingColumnName, referencedTableName, referencedColumnName);
    }

    @SuppressWarnings("DuplicatedCode")
    private Element saveAnalysis(String referencingTableName, String referencingColumnName, String referencedTableName, String referencedColumnName, Document document) {
        Element referenceConstraintNode = document.createElement(ANALYSIS_NAME);
        Element referencingTableNode = document.createElement(REFERENCING_TABLE_TAG);
        referencingTableNode.setTextContent(referencingTableName);
        referenceConstraintNode.appendChild(referencingTableNode);
        Element referencingColumnNode = document.createElement(REFERENCING_COLUMN_TAG);
        referencingColumnNode.setTextContent(referencingColumnName);
        referenceConstraintNode.appendChild(referencingColumnNode);
        Element referencedTableNode = document.createElement(REFERENCED_TABLE_TAG);
        referencedTableNode.setTextContent(referencedTableName);
        referenceConstraintNode.appendChild(referencedTableNode);
        Element referencedColumnNode = document.createElement(REFERENCED_COLUMN_TAG);
        referencedColumnNode.setTextContent(referencedColumnName);
        referenceConstraintNode.appendChild(referencedColumnNode);
        return referenceConstraintNode;
    }

    private void transformTable(Map<String, Map.Entry<String, String>> referencingTableEntry, Database database, Table referencingTable) {
        if (Objects.isNull(referencingTable)) {
            throw new RuntimeException("Unable to find table in the database");
        }
        referencingTableEntry.forEach((referencingColumnName, referencingColumnEntry) -> transformColumn(referencingTable, referencingTable.get(referencingColumnName), database.get(referencingColumnEntry.getKey()), referencingColumnEntry.getValue()));
    }

    @SuppressWarnings("DuplicatedCode")
    private void transformColumn(Table referencingTable, Column<?> referencingColumn, Table referencedTable, String referencedColumnName) {
        if (Objects.isNull(referencingColumn)) {
            throw new RuntimeException("Unable to find column in the table");
        }
        if (Objects.isNull(referencedTable)) {
            throw new RuntimeException("Unable to find table in the database");
        }
        Column<?> referencedColumn = referencedTable.get(referencedColumnName);
        if (Objects.isNull(referencedColumn)) {
            throw new RuntimeException("Unable to find column in the table");
        }
        ColumnValidator columnValidator = ((ReferenceConstraintValidator.ReferenceConstraintValidatorBuilder) ColumnValidatorBuilderFactory.getColumnValidatorBuilder(ANALYSIS_NAME))
                .referencingColumn(referencingColumn)
                .referencedColumn(referencedColumn)
                .build();
        this.configuration.referencePolicy().apply(referencingTable, columnValidator);
        referencingTable.addConstraint(createConstraint(referencingColumn, referencedTable, referencedColumn));
    }

    private Constraint createConstraint(Column<?> referencingColumn, Table referencedTable, Column<?> referencedColumn) {
        return ((ReferenceConstraint.ReferenceConstraintBuilder) ConstraintBuilderFactory.getConstraintBuilder(ReferenceConstraint.CONSTRAINT_NAME))
                .referencingColumn(referencingColumn)
                .referencedTable(referencedTable)
                .referencedColumn(referencedColumn)
                .build();
    }

    public static class ReferenceConstraintAnalysisBuilder implements AnalysisBuilder {
        private final ReferenceConstraintAnalysis referenceConstraintAnalysis;

        private ReferenceConstraintAnalysisBuilder() {
            this.referenceConstraintAnalysis = new ReferenceConstraintAnalysis();
        }

        public ReferenceConstraintAnalysisBuilder referenceConstraint(String referencingTableName, String referencingColumnName, String referencedTableName, String referencedColumnName) {
            this.referenceConstraintAnalysis.referenceConstraints.computeIfAbsent(referencingTableName, k -> new HashMap<>()).put(referencingColumnName, new AbstractMap.SimpleImmutableEntry<>(referencedTableName, referencedColumnName));
            return this;
        }

        @Override
        public ReferenceConstraintAnalysis build() {
            return this.referenceConstraintAnalysis;
        }
    }
}
