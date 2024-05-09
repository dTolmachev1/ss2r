package io.github.dtolmachev1.inference.analysis;

import io.github.dtolmachev1.data.database.Database;
import io.github.dtolmachev1.data.table.Table;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class TableNameAnalysis implements Analysis {
    public static final String ANALYSIS_NAME = "table-name";
    private static final String ANALYSIS_TAG = "analysis";
    private static final String ANALYSIS_NAME_TAG = "analysis-name";
    private static final String SOURCE_TABLE_NAME_TAG = "source-table-name";
    private static final String NEW_TABLE_NAME_TAG = "new-table-name";
    private final Map<String, String> tableNames;

    private TableNameAnalysis() {
        this.tableNames = new HashMap<>();
    }

    @Override
    public String name() {
        return ANALYSIS_NAME;
    }

    public static TableNameAnalysis load(NodeList tableNameEntries) {
        if (tableNameEntries.getLength() == 0) {
            throw new RuntimeException("Unable to load analyzes");
        }
        TableNameAnalysisBuilder tableNameAnalysisBuilder = builder();
        for (int i = 0; i < tableNameEntries.getLength(); i++) {
            loadAnalysis((Element) tableNameEntries.item(i), tableNameAnalysisBuilder);
        }
        return tableNameAnalysisBuilder.build();
    }

    @Override
    public Element save(Document document) {
        Element analysisNode = document.createElement(ANALYSIS_TAG);
        Element analysisNameNode = document.createElement(ANALYSIS_NAME_TAG);
        analysisNameNode.setTextContent(ANALYSIS_NAME);
        analysisNode.appendChild(analysisNameNode);
        this.tableNames.forEach((sourceTableName, newTableName) -> analysisNode.appendChild(saveAnalysis(sourceTableName, newTableName, document)));
        return analysisNode;
    }

    @Override
    public void transform(Database database) {
        this.tableNames.forEach((sourceTableName, newTableName) -> transformTable(database, database.get(sourceTableName), newTableName));
    }

    public static TableNameAnalysisBuilder builder() {
        return new TableNameAnalysisBuilder();
    }

    @SuppressWarnings("DuplicatedCode")
    private static void loadAnalysis(Element tableNameEntry, TableNameAnalysisBuilder tableNameAnalysisBuilder) {
        if (!tableNameEntry.hasChildNodes()) {
            throw new RuntimeException("Unable to load analyzes");
        }
        String sourceTableName = tableNameEntry.getElementsByTagName(SOURCE_TABLE_NAME_TAG).item(0).getTextContent();
        String newTableName = tableNameEntry.getElementsByTagName(NEW_TABLE_NAME_TAG).item(0).getTextContent();
        tableNameAnalysisBuilder.tableName(sourceTableName, newTableName);
    }

    @SuppressWarnings("DuplicatedCode")
    private Element saveAnalysis(String sourceTableName, String newTableName, Document document) {
        Element tableNameNode = document.createElement(ANALYSIS_NAME);
        Element sourceTableNameNode = document.createElement(SOURCE_TABLE_NAME_TAG);
        sourceTableNameNode.setTextContent(sourceTableName);
        tableNameNode.appendChild(sourceTableNameNode);
        Element newTableNameNode = document.createElement(NEW_TABLE_NAME_TAG);
        newTableNameNode.setTextContent(newTableName);
        tableNameNode.appendChild(newTableNameNode);
        return tableNameNode;
    }

    private void transformTable(Database database, Table table, String newTableName) {
        if (Objects.isNull(table)) {
            throw new RuntimeException("Unable to find table in the database");
        }
        database.remove(table.getName());
        table.setName(newTableName);
        database.add(table);
    }

    public static class TableNameAnalysisBuilder implements AnalysisBuilder {
        private final TableNameAnalysis tableNameAnalysis;

        private TableNameAnalysisBuilder() {
            this.tableNameAnalysis = new TableNameAnalysis();
        }

        public TableNameAnalysisBuilder tableName(String sourceTableName, String newTableName) {
            this.tableNameAnalysis.tableNames.put(sourceTableName, newTableName);
            return this;
        }

        @Override
        public TableNameAnalysis build() {
            return this.tableNameAnalysis;
        }
    }
}
