package io.github.dtolmachev1.inference.analysis;

import io.github.dtolmachev1.configuration.Configuration;
import io.github.dtolmachev1.configuration.XmlConfiguration;
import io.github.dtolmachev1.data.column.Column;
import io.github.dtolmachev1.data.column.ColumnType;
import io.github.dtolmachev1.data.column.ColumnTypeFactory;
import io.github.dtolmachev1.data.column.DoubleType;
import io.github.dtolmachev1.data.column.GenericColumn;
import io.github.dtolmachev1.data.column.IntegerType;
import io.github.dtolmachev1.data.column.StringType;
import io.github.dtolmachev1.data.database.Database;
import io.github.dtolmachev1.data.table.Table;
import io.github.dtolmachev1.inference.validator.ColumnTypeValidator;
import io.github.dtolmachev1.inference.validator.ColumnValidator;
import io.github.dtolmachev1.inference.validator.ColumnValidatorBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ColumnTypeAnalysis implements Analysis {
    public static final String ANALYSIS_NAME = "column-type";
    private static final String ANALYSIS_TAG = "analysis";
    private static final String ANALYSIS_NAME_TAG = "analysis-name";
    private static final String TABLE_TAG = "table";
    private static final String COLUMN_TAG = "column";
    private static final String TYPE_TAG = "type";
    private static final String LENGTH_TAG = "length";
    private final Map<String, Map<String, ColumnType>> columnTypes;
    private final Configuration configuration;

    private ColumnTypeAnalysis() {
        this.columnTypes = new HashMap<>();
        this.configuration = XmlConfiguration.newInstance();
    }

    @Override
    public String name() {
        return ANALYSIS_NAME;
    }

    public static ColumnTypeAnalysis load(NodeList columnTypeEntries) {
        if (columnTypeEntries.getLength() == 0) {
            throw new RuntimeException("Unable to load analyzes");
        }
        ColumnTypeAnalysisBuilder columnTypeAnalysisBuilder = builder();
        for (int i = 0; i < columnTypeEntries.getLength(); i++) {
            loadAnalysis((Element) columnTypeEntries.item(i), columnTypeAnalysisBuilder);
        }
        return columnTypeAnalysisBuilder.build();
    }

    @Override
    public Element save(Document document) {
        Element analysisNode = document.createElement(ANALYSIS_TAG);
        Element analysisNameNode = document.createElement(ANALYSIS_NAME_TAG);
        analysisNameNode.setTextContent(ANALYSIS_NAME);
        analysisNode.appendChild(analysisNameNode);
        this.columnTypes.forEach((tableName, tableEntry) -> tableEntry.forEach((columnName, columnType) -> analysisNode.appendChild(saveAnalysis(tableName, columnName, columnType, document))));
        return analysisNode;
    }

    @Override
    public void transform(Database database) {
        this.columnTypes.forEach((tableName, tableEntry) -> transformTable(tableEntry, database.get(tableName)));
    }

    public static ColumnTypeAnalysisBuilder builder() {
        return new ColumnTypeAnalysisBuilder();
    }

    @SuppressWarnings("DuplicatedCode")
    private static void loadAnalysis(Element columnTypeEntry, ColumnTypeAnalysisBuilder columnTypeAnalysisBuilder) {
        if (!columnTypeEntry.hasChildNodes()) {
            throw new RuntimeException("Unable to load analyzes");
        }
        String tableName = columnTypeEntry.getElementsByTagName(TABLE_TAG).item(0).getTextContent();
        String columnName = columnTypeEntry.getElementsByTagName(COLUMN_TAG).item(0).getTextContent();
        ColumnType columnType = ColumnTypeFactory.getColumnType(columnTypeEntry.getElementsByTagName(TYPE_TAG).item(0).getTextContent());
        if (columnType.name().equals(StringType.TYPE_NAME)) {
            StringType stringType = (StringType) columnType;
            stringType.setMaxLength(Integer.parseInt(columnTypeEntry.getElementsByTagName(LENGTH_TAG).item(0).getTextContent()));
        }
        columnTypeAnalysisBuilder.columnType(tableName, columnName, columnType);
    }

    @SuppressWarnings("DuplicatedCode")
    private Element saveAnalysis(String tableName, String columnName, ColumnType columnType, Document document) {
        Element columnTypeNode = document.createElement(ANALYSIS_NAME);
        Element tableNode = document.createElement(TABLE_TAG);
        tableNode.setTextContent(tableName);
        columnTypeNode.appendChild(tableNode);
        Element columnNode = document.createElement(COLUMN_TAG);
        columnNode.setTextContent(columnName);
        columnTypeNode.appendChild(columnNode);
        Element typeNode = document.createElement(TYPE_TAG);
        typeNode.setTextContent(columnType.name());
        columnTypeNode.appendChild(typeNode);
        if (columnType.name().equals(StringType.TYPE_NAME)) {
            Element lengthNode = document.createElement(LENGTH_TAG);
            lengthNode.setTextContent(Integer.toString(((StringType) columnType).getMaxLength()));
            columnTypeNode.appendChild(lengthNode);
        }
        return columnTypeNode;
    }

    @SuppressWarnings("unchecked")
    private void transformTable(Map<String, ColumnType> tableEntry, Table table) {
        if (Objects.isNull(table)) {
            throw new RuntimeException("Unable to find table in the database");
        }
        tableEntry.forEach((columnName, columnType) -> table.set(transformColumn(table, (Column<String>) table.get(columnName), columnType)));
    }

    private Column<?> transformColumn(Table table, Column<String> column, ColumnType columnType) {
        if (Objects.isNull(column)) {
            throw new RuntimeException("Unable to find column in the table");
        }
        ColumnValidator columnValidator = ((ColumnTypeValidator.ColumnTypeValidatorBuilder) ColumnValidatorBuilderFactory.getColumnValidatorBuilder(ANALYSIS_NAME))
                .column(column)
                .columnType(columnType)
                .build();
        this.configuration.typePolicy().apply(table, columnValidator);
        return createColumn(column, columnType);
    }

    @SuppressWarnings("DuplicatedCode")
    private Column<?> createColumn(Column<String> sourceColumn, ColumnType columnType) {
        if (columnType.name().equals(IntegerType.TYPE_NAME)) {
            Column<Integer> newColumn = new GenericColumn<>(sourceColumn.getName(), columnType);
            sourceColumn.forEach(entry -> newColumn.add(entry.getKey(), Integer.valueOf(entry.getValue())));
            return newColumn;
        }
        if (columnType.name().equals(DoubleType.TYPE_NAME)) {
            Column<Double> newColumn = new GenericColumn<>(sourceColumn.getName(), columnType);
            sourceColumn.forEach(entry -> newColumn.add(entry.getKey(), Double.valueOf(entry.getValue())));
            return newColumn;
        }
        if (columnType.name().equals(StringType.TYPE_NAME)) {
            StringType stringType = (StringType) columnType;
            stringType.setMaxLength(sourceColumn.stream()
                    .map(entry -> entry.getValue().length())
                    .max(Integer::compare)
                    .orElse(1));
        }
        sourceColumn.setType(columnType);
        return sourceColumn;
    }

    public static class ColumnTypeAnalysisBuilder implements Analysis.AnalysisBuilder {
        private final ColumnTypeAnalysis columnTypeAnalysis;

        private ColumnTypeAnalysisBuilder() {
            this.columnTypeAnalysis = new ColumnTypeAnalysis();
        }

        public ColumnTypeAnalysisBuilder columnType(String tableName, String columnName, ColumnType columnType) {
            this.columnTypeAnalysis.columnTypes.computeIfAbsent(tableName, k -> new HashMap<>()).put(columnName, columnType);
            return this;
        }

        @Override
        public ColumnTypeAnalysis build() {
            return this.columnTypeAnalysis;
        }
    }
}
