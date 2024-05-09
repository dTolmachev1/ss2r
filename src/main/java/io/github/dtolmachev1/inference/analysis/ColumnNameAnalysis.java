package io.github.dtolmachev1.inference.analysis;

import io.github.dtolmachev1.data.column.Column;
import io.github.dtolmachev1.data.database.Database;
import io.github.dtolmachev1.data.table.Table;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ColumnNameAnalysis implements Analysis {
    public static final String ANALYSIS_NAME = "column-name";
    private static final String ANALYSIS_TAG = "analysis";
    private static final String ANALYSIS_NAME_TAG = "analysis-name";
    private static final String TABLE_TAG = "table";
    private static final String SOURCE_COLUMN_NAME_TAG = "source-column-name";
    private static final String NEW_COLUMN_NAME_TAG = "new-column-name";
    private final Map<String, Map<String, String>> columnNames;

    private ColumnNameAnalysis() {
        this.columnNames = new HashMap<>();
    }

    @Override
    public String name() {
        return ANALYSIS_NAME;
    }

    public static ColumnNameAnalysis load(NodeList columnNameEntries) {
        if (columnNameEntries.getLength() == 0) {
            throw new RuntimeException("Unable to load analyzes");
        }
        ColumnNameAnalysisBuilder columnNameAnalysisBuilder = builder();
        for (int i = 0; i < columnNameEntries.getLength(); i++) {
            loadAnalysis((Element) columnNameEntries.item(i), columnNameAnalysisBuilder);
        }
        return columnNameAnalysisBuilder.build();
    }

    @Override
    public Element save(Document document) {
        Element analysisNode = document.createElement(ANALYSIS_TAG);
        Element analysisNameNode = document.createElement(ANALYSIS_NAME_TAG);
        analysisNameNode.setTextContent(ANALYSIS_NAME);
        analysisNode.appendChild(analysisNameNode);
        this.columnNames.forEach((tableName, tableEntry) -> tableEntry.forEach((sourceColumnName, newColumnName) -> analysisNode.appendChild(saveAnalysis(tableName, sourceColumnName, newColumnName, document))));
        return analysisNode;
    }

    @Override
    public void transform(Database database) {
        this.columnNames.forEach((tableName, tableEntry) -> transformTable(tableEntry, database.get(tableName)));
    }

    public static ColumnNameAnalysisBuilder builder() {
        return new ColumnNameAnalysisBuilder();
    }

    @SuppressWarnings("DuplicatedCode")
    private static void loadAnalysis(Element columnNameEntry, ColumnNameAnalysisBuilder columnNameAnalysisBuilder) {
        if (!columnNameEntry.hasChildNodes()) {
            throw new RuntimeException("Unable to load analyzes");
        }
        String tableName = columnNameEntry.getElementsByTagName(TABLE_TAG).item(0).getTextContent();
        String sourceColumnName = columnNameEntry.getElementsByTagName(SOURCE_COLUMN_NAME_TAG).item(0).getTextContent();
        String newColumnName = columnNameEntry.getElementsByTagName(NEW_COLUMN_NAME_TAG).item(0).getTextContent();
        columnNameAnalysisBuilder.columnName(tableName, sourceColumnName, newColumnName);
    }

    @SuppressWarnings("DuplicatedCode")
    private Element saveAnalysis(String tableName, String sourceColumnName, String newColumnName, Document document) {
        Element columnNameNode = document.createElement(ANALYSIS_NAME);
        Element tableNode = document.createElement(TABLE_TAG);
        tableNode.setTextContent(tableName);
        columnNameNode.appendChild(tableNode);
        Element sourceColumnNameNode = document.createElement(SOURCE_COLUMN_NAME_TAG);
        sourceColumnNameNode.setTextContent(sourceColumnName);
        columnNameNode.appendChild(sourceColumnNameNode);
        Element newColumnNameNode = document.createElement(NEW_COLUMN_NAME_TAG);
        newColumnNameNode.setTextContent(newColumnName);
        columnNameNode.appendChild(newColumnNameNode);
        return columnNameNode;
    }

    private void transformTable(Map<String, String> tableEntry, Table table) {
        if (Objects.isNull(table)) {
            throw new RuntimeException("Unable to find table in the database");
        }
        List<Column<?>> columns = table.stream().map(Map.Entry::getValue).collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
        tableEntry.forEach((sourceColumnName, newColumnName) -> transformColumn(table.get(sourceColumnName), newColumnName));
        table.clear();
        columns.forEach(table::add);
    }

    private void transformColumn(Column<?> column, String newColumnName) {
        if (Objects.isNull(column)) {
            throw new RuntimeException("Unable to find column in the table");
        }
        column.setName(newColumnName);
    }

    public static class ColumnNameAnalysisBuilder implements AnalysisBuilder {
        private final ColumnNameAnalysis columnNameAnalysis;

        private ColumnNameAnalysisBuilder() {
            this.columnNameAnalysis = new ColumnNameAnalysis();
        }

        public ColumnNameAnalysisBuilder columnName(String tableName, String sourceColumnName, String newColumnName) {
            this.columnNameAnalysis.columnNames.computeIfAbsent(tableName, k -> new HashMap<>()).put(sourceColumnName, newColumnName);
            return this;
        }

        @Override
        public ColumnNameAnalysis build() {
            return this.columnNameAnalysis;
        }
    }
}
