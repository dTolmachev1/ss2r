package io.github.dtolmachev1.inference.analysis;

import io.github.dtolmachev1.configuration.Configuration;
import io.github.dtolmachev1.configuration.XmlConfiguration;
import io.github.dtolmachev1.data.column.Column;
import io.github.dtolmachev1.data.column.GenericColumn;
import io.github.dtolmachev1.data.database.Database;
import io.github.dtolmachev1.data.table.GenericTable;
import io.github.dtolmachev1.data.table.Table;
import io.github.dtolmachev1.inference.validator.SimilarTablesValidator;
import io.github.dtolmachev1.inference.validator.TableValidator;
import io.github.dtolmachev1.inference.validator.TableValidatorBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SimilarTablesAnalysis implements Analysis {
    public static final String ANALYSIS_NAME = "similar-tables";
    private static final String ANALYSIS_TAG = "analysis";
    private static final String ANALYSIS_NAME_TAG = "analysis-name";
    private static final String SOURCE_TABLES_TAG = "source-tables";
    private static final String SOURCE_TABLE_TAG = "source-table";
    private static final String NEW_TABLE_NAME_TAG = "new-table-name";
    private final Map<String, List<String>> similarTables;
    private final Configuration configuration;

    private SimilarTablesAnalysis() {
        this.similarTables = new HashMap<>();
        this.configuration = XmlConfiguration.newInstance();
    }

    @Override
    public String name() {
        return ANALYSIS_NAME;
    }

    public static SimilarTablesAnalysis load(NodeList similarTablesEntries) {
        if (similarTablesEntries.getLength() == 0) {
            throw new RuntimeException("Unable to load analyzes");
        }
        SimilarTablesAnalysisBuilder similarTablesAnalysisBuilder = builder();
        for (int i = 0; i < similarTablesEntries.getLength(); i++) {
            loadAnalysis((Element) similarTablesEntries.item(i), similarTablesAnalysisBuilder);
        }
        return similarTablesAnalysisBuilder.build();
    }

    @Override
    public Element save(Document document) {
        Element analysisNode = document.createElement(ANALYSIS_TAG);
        Element analysisNameNode = document.createElement(ANALYSIS_NAME_TAG);
        analysisNameNode.setTextContent(ANALYSIS_NAME);
        analysisNode.appendChild(analysisNameNode);
        similarTables.forEach((newTableName, sourceTableNames) -> analysisNode.appendChild(saveAnalysis(sourceTableNames, newTableName, document)));
        return analysisNode;
    }

    @Override
    public void transform(Database database) {
        if (!this.configuration.mergeSimilarTables()) {
            return;
        }
        similarTables.forEach((newTableName, sourceTableNames) -> transformTable(database, sourceTableNames.stream().map(database::get).toList(), newTableName));
    }

    public static SimilarTablesAnalysisBuilder builder() {
        return new SimilarTablesAnalysisBuilder();
    }

    @SuppressWarnings("DuplicatedCode")
    private static void loadAnalysis(Element similarTablesEntry, SimilarTablesAnalysisBuilder similarTablesAnalysisBuilder) {
        if (!similarTablesEntry.hasChildNodes()) {
            throw new RuntimeException("Unable to load analyzes");
        }
        List<String> sourceTables = loadSourceTables(similarTablesEntry.getElementsByTagName(SOURCE_TABLE_TAG));
        String newTableName = similarTablesEntry.getElementsByTagName(NEW_TABLE_NAME_TAG).item(0).getTextContent();
        similarTablesAnalysisBuilder.similarTables(sourceTables, newTableName);
    }

    private static List<String> loadSourceTables(NodeList sourceTableEntries) {
        if (sourceTableEntries.getLength() == 0) {
            throw new RuntimeException("Unable to load analyzes");
        }
        List<String> sourceTables = new ArrayList<>();
        for (int i = 0; i < sourceTableEntries.getLength(); i++) {
            sourceTables.add(sourceTableEntries.item(i).getTextContent());
        }
        return sourceTables;
    }

    @SuppressWarnings("DuplicatedCode")
    private Element saveAnalysis(List<String> sourceTableNames, String newTableName, Document document) {
        Element similarTablesNode = document.createElement(ANALYSIS_NAME);
        Element sourceTablesNode = document.createElement(SOURCE_TABLES_TAG);
        sourceTableNames.forEach(sourceTableName -> sourceTablesNode.appendChild(saveSourceTable(sourceTableName, document)));
        similarTablesNode.appendChild(sourceTablesNode);
        Element newTableNode = document.createElement(NEW_TABLE_NAME_TAG);
        newTableNode.setTextContent(newTableName);
        similarTablesNode.appendChild(newTableNode);
        return similarTablesNode;
    }

    private Element saveSourceTable(String sourceTableName, Document document) {
        Element sourceTableNode = document.createElement(SOURCE_TABLE_TAG);
        sourceTableNode.setTextContent(sourceTableName);
        return sourceTableNode;
    }

    @SuppressWarnings("unchecked")
    private void transformTable(Database database, List<Table> sourceTables, String newTableName) {
        if (sourceTables.stream().anyMatch(Objects::isNull)) {
            throw new RuntimeException("Unable to find table in the database");
        }
        TableValidator tableValidator = ((SimilarTablesValidator.SimilarTablesValidatorBuilder) TableValidatorBuilderFactory.getTableValidatorBuilder(ANALYSIS_NAME))
                .similarTables(sourceTables)
                .build();
        sourceTables.forEach(table -> this.configuration.similarTablesPolicy().apply(table, tableValidator));
        Table newTable = new GenericTable(newTableName);
        for (Table sourceTable : sourceTables) {
            for (Map.Entry<String, Column<?>> columnEntry : sourceTable) {
                Column<String> stringColumn = (Column<String>) columnEntry.getValue();
                Column<String> newColumn = (Column<String>) newTable.get(columnEntry.getKey());
                if (Objects.isNull(newColumn)) {
                    newColumn = new GenericColumn<>(columnEntry.getKey());
                    newTable.add(newColumn);
                }
                for (Map.Entry<Integer, String> valueEntry : stringColumn) {
                    newColumn.add(valueEntry.getValue());
                }
            }
            sourceTable.sources().forEach(newTable::addSource);
            database.remove(sourceTable.getName());
        }
        database.add(newTable);
    }

    public static class SimilarTablesAnalysisBuilder implements AnalysisBuilder {
        private final SimilarTablesAnalysis similarTablesAnalysis;

        private SimilarTablesAnalysisBuilder() {
            this.similarTablesAnalysis = new SimilarTablesAnalysis();
        }

        public SimilarTablesAnalysisBuilder similarTables(List<String> sourceTableNames, String newTableName) {
            this.similarTablesAnalysis.similarTables.put(newTableName, sourceTableNames);
            return this;
        }

        @Override
        public SimilarTablesAnalysis build() {
            return this.similarTablesAnalysis;
        }
    }
}
