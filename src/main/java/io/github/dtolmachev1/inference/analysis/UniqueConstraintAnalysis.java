package io.github.dtolmachev1.inference.analysis;

import io.github.dtolmachev1.configuration.Configuration;
import io.github.dtolmachev1.configuration.XmlConfiguration;
import io.github.dtolmachev1.data.column.Column;
import io.github.dtolmachev1.data.database.Database;
import io.github.dtolmachev1.data.table.Constraint;
import io.github.dtolmachev1.data.table.ConstraintBuilderFactory;
import io.github.dtolmachev1.data.table.Table;
import io.github.dtolmachev1.data.table.UniqueConstraint;
import io.github.dtolmachev1.inference.validator.UniqueConstraintValidator;
import io.github.dtolmachev1.inference.validator.ColumnValidator;
import io.github.dtolmachev1.inference.validator.ColumnValidatorBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class UniqueConstraintAnalysis implements Analysis {
    public static final String ANALYSIS_NAME = "unique-constraint";
    private static final String ANALYSIS_TAG = "analysis";
    private static final String ANALYSIS_NAME_TAG = "analysis-name";
    private static final String TABLE_TAG = "table";
    private static final String COLUMN_TAG = "column";
    private final Map<String, String> uniqueConstraints;
    private final Configuration configuration;

    private UniqueConstraintAnalysis() {
        this.uniqueConstraints = new HashMap<>();
        this.configuration = XmlConfiguration.newInstance();
    }

    @Override
    public String name() {
        return ANALYSIS_NAME;
    }

    public static UniqueConstraintAnalysis load(NodeList uniqueConstraintEntries) {
        if (uniqueConstraintEntries.getLength() == 0) {
            throw new RuntimeException("Unable to load analyzes");
        }
        UniqueConstraintAnalysisBuilder uniqueConstraintAnalysisBuilder = builder();
        for (int i = 0; i < uniqueConstraintEntries.getLength(); i++) {
            loadAnalysis((Element) uniqueConstraintEntries.item(i), uniqueConstraintAnalysisBuilder);
        }
        return uniqueConstraintAnalysisBuilder.build();
    }

    @Override
    public Element save(Document document) {
        Element analysisNode = document.createElement(ANALYSIS_TAG);
        Element analysisNameNode = document.createElement(ANALYSIS_NAME_TAG);
        analysisNameNode.setTextContent(ANALYSIS_NAME);
        analysisNode.appendChild(analysisNameNode);
        this.uniqueConstraints.forEach((tableName, columnName) -> analysisNode.appendChild(saveAnalysis(tableName, columnName, document)));
        return analysisNode;
    }

    @Override
    public void transform(Database database) {
        this.uniqueConstraints.forEach((tableName, columnName) -> transformTable(database.get(tableName), columnName));
    }

    public static UniqueConstraintAnalysisBuilder builder() {
        return new UniqueConstraintAnalysisBuilder();
    }

    @SuppressWarnings("DuplicatedCode")
    private static void loadAnalysis(Element uniqueConstraintEntry, UniqueConstraintAnalysisBuilder uniqueConstraintAnalysisBuilder) {
        if (!uniqueConstraintEntry.hasChildNodes()) {
            throw new RuntimeException("Unable to read analyzes");
        }
        String tableName = uniqueConstraintEntry.getElementsByTagName(TABLE_TAG).item(0).getTextContent();
        String columnName = uniqueConstraintEntry.getElementsByTagName(COLUMN_TAG).item(0).getTextContent();
        uniqueConstraintAnalysisBuilder.uniqueConstraint(tableName, columnName);
    }

    @SuppressWarnings("DuplicatedCode")
    private Element saveAnalysis(String tableName, String columnName, Document document) {
        Element uniqueConstraintNode = document.createElement(ANALYSIS_NAME);
        Element tableNode = document.createElement(TABLE_TAG);
        tableNode.setTextContent(tableName);
        uniqueConstraintNode.appendChild(tableNode);
        Element columnNode = document.createElement(COLUMN_TAG);
        columnNode.setTextContent(columnName);
        uniqueConstraintNode.appendChild(columnNode);
        return uniqueConstraintNode;
    }

    private void transformTable(Table table, String columnName) {
        if (Objects.isNull(table)) {
            throw new RuntimeException("Unable to find table in the database");
        }
        transformColumn(table, table.get(columnName));
    }

    private void transformColumn(Table table, Column<?> column) {
        if (Objects.isNull(column)) {
            throw new RuntimeException("Unable to find column in the table");
        }
        ColumnValidator columnValidator = ((UniqueConstraintValidator.UniqueConstraintValidatorBuilder) ColumnValidatorBuilderFactory.getColumnValidatorBuilder(ANALYSIS_NAME))
                .column(column)
                        .build();
        this.configuration.uniquePolicy().apply(table, columnValidator);
        table.addConstraint(createConstraint(column));
    }

    private Constraint createConstraint(Column<?> column) {
        return ((UniqueConstraint.UniqueConstraintBuilder) ConstraintBuilderFactory.getConstraintBuilder(UniqueConstraint.CONSTRAINT_NAME))
                .column(column)
                .build();
    }

    public static class UniqueConstraintAnalysisBuilder implements AnalysisBuilder {
        private final UniqueConstraintAnalysis uniqueConstraintAnalysis;

        private UniqueConstraintAnalysisBuilder() {
            this.uniqueConstraintAnalysis = new UniqueConstraintAnalysis();
        }

        public UniqueConstraintAnalysisBuilder uniqueConstraint(String tableName, String columnName) {
            this.uniqueConstraintAnalysis.uniqueConstraints.put(tableName, columnName);
            return this;
        }

        @Override
        public UniqueConstraintAnalysis build() {
            return this.uniqueConstraintAnalysis;
        }
    }
}
