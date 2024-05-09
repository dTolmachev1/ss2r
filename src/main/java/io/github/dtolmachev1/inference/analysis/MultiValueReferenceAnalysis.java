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
import io.github.dtolmachev1.data.table.Constraint;
import io.github.dtolmachev1.data.table.ConstraintBuilderFactory;
import io.github.dtolmachev1.data.table.GenericTable;
import io.github.dtolmachev1.data.table.ReferenceConstraint;
import io.github.dtolmachev1.data.table.Table;
import io.github.dtolmachev1.data.table.UniqueConstraint;
import io.github.dtolmachev1.inference.validator.ColumnTypeValidator;
import io.github.dtolmachev1.inference.validator.ColumnValidator;
import io.github.dtolmachev1.inference.validator.ColumnValidatorBuilderFactory;
import io.github.dtolmachev1.inference.validator.MultiValueReferenceValidator;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

public class MultiValueReferenceAnalysis implements Analysis {
    public static final String ANALYSIS_NAME = "multi-value-reference";
    private static final String ANALYSIS_TAG = "analysis";
    private static final String ANALYSIS_NAME_TAG = "analysis-name";
    private static final String REFERENCING_TABLE_TAG = "referencing-table";
    private static final String REFERENCING_COLUMN_TAG = "referencing-column";
    private static final String MULTI_VALUE_REFERENCE_SEPARATOR_TAG = "multi-value-reference-separator";
    private static final String REFERENCED_TABLE_TAG = "referenced-table";
    private static final String REFERENCED_COLUMN_TAG = "referenced-column";
    private static final String NEW_TABLE_NAME_TAG = "new-table-name";
    private static final String NEW_ID_COLUMN_NAME_TAG = "new-id-column-name";
    private static final String NEW_REFERENCING_COLUMN_NAME_TAG = "new-referencing-column-name";
    private static final String NEW_REFERENCED_COLUMN_NAME_TAG = "new-referenced-column-name";
    private final Map<String, Map<String, List<String>>> multiValueReferences;
    private final Configuration configuration;

    private MultiValueReferenceAnalysis() {
        this.multiValueReferences = new HashMap<>();
        this.configuration = XmlConfiguration.newInstance();
    }

    @Override
    public String name() {
        return ANALYSIS_NAME;
    }

    public static MultiValueReferenceAnalysis load(NodeList multiValueReferenceEntries) {
        if (multiValueReferenceEntries.getLength() == 0) {
            throw new RuntimeException("Unable to load analyzes");
        }
        MultiValueReferenceAnalysisBuilder multiValueReferenceAnalysisBuilder = builder();
        for (int i = 0; i < multiValueReferenceEntries.getLength(); i++) {
            loadAnalysis((Element) multiValueReferenceEntries.item(i), multiValueReferenceAnalysisBuilder);
        }
        return multiValueReferenceAnalysisBuilder.build();
    }

    @Override
    public Element save(Document document) {
        Element analysisNode = document.createElement(ANALYSIS_TAG);
        Element analysisNameNode = document.createElement(ANALYSIS_NAME_TAG);
        analysisNameNode.setTextContent(ANALYSIS_NAME);
        analysisNode.appendChild(analysisNameNode);
        this.multiValueReferences.forEach((referencingTableName, referencingTableEntry) -> referencingTableEntry.forEach((referencingColumnName, referencingColumnEntry) -> analysisNode.appendChild(saveAnalysis(referencingTableName, referencingColumnName, referencingColumnEntry.get(0), referencingColumnEntry.get(1), referencingColumnEntry.get(2), referencingColumnEntry.get(3), referencingColumnEntry.get(4), referencingColumnEntry.get(5), referencingColumnEntry.get(6), document))));
        return analysisNode;
    }

    @Override
    public void transform(Database database) {
        this.multiValueReferences.forEach((referencingTableName, referencingTableEntry) -> transformTable(referencingTableEntry, database, database.get(referencingTableName)));
    }

    public static MultiValueReferenceAnalysisBuilder builder() {
        return new MultiValueReferenceAnalysisBuilder();
    }

    @SuppressWarnings("DuplicatedCode")
    private static void loadAnalysis(Element multiValueReferenceEntry, MultiValueReferenceAnalysisBuilder multiValueReferenceAnalysisBuilder) {
        if (!multiValueReferenceEntry.hasChildNodes()) {
            throw new RuntimeException("Unable to load analyzes");
        }
        String referencingTableName = multiValueReferenceEntry.getElementsByTagName(REFERENCING_TABLE_TAG).item(0).getTextContent();
        String referencingColumnName = multiValueReferenceEntry.getElementsByTagName(REFERENCING_COLUMN_TAG).item(0).getTextContent();
        String multiValueReferenceSeparator = multiValueReferenceEntry.getElementsByTagName(MULTI_VALUE_REFERENCE_SEPARATOR_TAG).item(0).getTextContent();
        String referencedTableName = multiValueReferenceEntry.getElementsByTagName(REFERENCED_TABLE_TAG).item(0).getTextContent();
        String referencedColumnName = multiValueReferenceEntry.getElementsByTagName(REFERENCED_COLUMN_TAG).item(0).getTextContent();
        String newTableName = multiValueReferenceEntry.getElementsByTagName(NEW_TABLE_NAME_TAG).item(0).getTextContent();
        String newIdColumnName = multiValueReferenceEntry.getElementsByTagName(NEW_ID_COLUMN_NAME_TAG).item(0).getTextContent();
        String newReferencingColumnName = multiValueReferenceEntry.getElementsByTagName(NEW_REFERENCING_COLUMN_NAME_TAG).item(0).getTextContent();
        String newReferencedColumnName = multiValueReferenceEntry.getElementsByTagName(NEW_REFERENCED_COLUMN_NAME_TAG).item(0).getTextContent();
        multiValueReferenceAnalysisBuilder.multiValueReference(referencingTableName, referencingColumnName, multiValueReferenceSeparator, referencedTableName, referencedColumnName, newTableName, newIdColumnName, newReferencingColumnName, newReferencedColumnName);
    }

    @SuppressWarnings("DuplicatedCode")
    private Element saveAnalysis(String referencingTableName, String referencingColumnName, String multiValueReferenceSeparator, String referencedTableName, String referencedColumnName, String newTableName, String newIdColumnName, String newReferencingColumnName, String newReferencedColumnName, Document document) {
        Element multiValueReferenceNode = document.createElement(ANALYSIS_NAME);
        Element referencingTableNode = document.createElement(REFERENCING_TABLE_TAG);
        referencingTableNode.setTextContent(referencingTableName);
        multiValueReferenceNode.appendChild(referencingTableNode);
        Element referencingColumnNode = document.createElement(REFERENCING_COLUMN_TAG);
        referencingColumnNode.setTextContent(referencingColumnName);
        multiValueReferenceNode.appendChild(referencingColumnNode);
        Element multiValueReferenceSeparatorNode = document.createElement(MULTI_VALUE_REFERENCE_SEPARATOR_TAG);
        multiValueReferenceSeparatorNode.setTextContent(multiValueReferenceSeparator);
        multiValueReferenceNode.appendChild(multiValueReferenceSeparatorNode);
        Element referencedTableNode = document.createElement(REFERENCED_TABLE_TAG);
        referencedTableNode.setTextContent(referencedTableName);
        multiValueReferenceNode.appendChild(referencedTableNode);
        Element referencedColumnNode = document.createElement(REFERENCED_COLUMN_TAG);
        referencedColumnNode.setTextContent(referencedColumnName);
        multiValueReferenceNode.appendChild(referencedColumnNode);
        Element newTableNameNode = document.createElement(NEW_TABLE_NAME_TAG);
        newTableNameNode.setTextContent(newTableName);
        multiValueReferenceNode.appendChild(newTableNameNode);
        Element newIdColumnNameNode = document.createElement(NEW_ID_COLUMN_NAME_TAG);
        newIdColumnNameNode.setTextContent(newIdColumnName);
        multiValueReferenceNode.appendChild(newIdColumnNameNode);
        Element newReferencingColumnNameNode = document.createElement(NEW_REFERENCING_COLUMN_NAME_TAG);
        newReferencingColumnNameNode.setTextContent(newReferencingColumnName);
        multiValueReferenceNode.appendChild(newReferencingColumnNameNode);
        Element newReferencedColumnNameNode = document.createElement(NEW_REFERENCED_COLUMN_NAME_TAG);
        newReferencedColumnNameNode.setTextContent(newReferencedColumnName);
        multiValueReferenceNode.appendChild(newReferencedColumnNameNode);
        return multiValueReferenceNode;
    }

    private void transformTable(Map<String, List<String>> referencingTableEntry, Database database, Table referencingTable) {
        if (Objects.isNull(referencingTable)) {
            throw new RuntimeException("Unable to find table in the database");
        }
        Optional<Column<?>> columnWithUniqueConstraint = referencingTable.columnWithUniqueConstraint();
        if (columnWithUniqueConstraint.isEmpty()) {
            throw new RuntimeException("Unable to find column in the table");
        }
        referencingTableEntry.forEach((referencingColumnName, referencingColumnEntry) -> transformColumn(database, referencingTable, columnWithUniqueConstraint.get(), referencingTable.get(referencingColumnName), referencingColumnEntry.get(0), database.get(referencingColumnEntry.get(1)), referencingColumnEntry.get(2), referencingColumnEntry.get(3), referencingColumnEntry.get(4), referencingColumnEntry.get(5), referencingColumnEntry.get(6)));
    }

    @SuppressWarnings({"DuplicatedCode", "unchecked"})
    private void transformColumn(Database database, Table referencingTable, Column<?> columnWithUniqueConstraint, Column<?> referencingColumn, String multiValueReferenceSeparator, Table referencedTable, String referencedColumnName, String newTableName, String newIdColumnName, String newReferencingColumnName, String newReferencedColumnName) {
        if (Objects.isNull(referencingColumn)) {
            throw new RuntimeException("Unable to find column in the table");
        }
        if (Objects.isNull(referencingTable)) {
            throw new RuntimeException("Unable to find table in the database");
        }
        Column<?> referencedColumn = referencedTable.get(referencedColumnName);
        if (Objects.isNull(referencedColumn)) {
            throw new RuntimeException("Unable to find column in the table");
        }
        Table newTable = createTable(referencingTable, columnWithUniqueConstraint, (Column<String>) referencingColumn, multiValueReferenceSeparator, referencedTable, referencedColumn, newTableName, newIdColumnName, newReferencingColumnName, newReferencedColumnName);
        referencingTable.remove(referencingColumn.getName());
        database.add(newTable);
    }

    private Table createTable(Table referencingTable, Column<?> columnWithUniqueConstraint, Column<String> referencingColumn, String multiValueReferenceSeparator, Table referencedTable, Column<?> referencedColumn, String newTableName, String newIdColumnName, String newReferencingColumnName, String newReferencedColumnName) {
        Table newTable = new GenericTable(newTableName);
        Map<Integer, String[]> splitValues = new LinkedHashMap<>();
        referencingColumn.forEach(entry -> splitValues.put(entry.getKey(), entry.getValue().split(Pattern.quote(multiValueReferenceSeparator))));
        Column<Integer> newIdColumn = createIdColumn(splitValues, newIdColumnName);
        newTable.add(newIdColumn);
        Column<?> newReferencingColumn = createReferencingColumn(columnWithUniqueConstraint, splitValues, newReferencingColumnName);
        newTable.add(newReferencingColumn);
        Column<String> newReferencedColumn = createReferencedColumn(splitValues, newReferencedColumnName);
        newTable.add(newReferencedColumn);
        validateTable(referencedColumn, newTable, newReferencedColumn);
        newTable.addConstraint(createUniqueConstraint(newIdColumn));
        newTable.addConstraint(createReferenceConstraint(newReferencingColumn, referencingTable, columnWithUniqueConstraint));
        newTable.addConstraint(createReferenceConstraint(newTable.get(newReferencedColumnName), referencedTable, referencedColumn));
        return newTable;
    }

    private Column<Integer> createIdColumn(Map<Integer, String[]> splitValues, String newIdColumnName) {
        Column<Integer> newIdColumn = new GenericColumn<>(newIdColumnName);
        for (Map.Entry<Integer, String[]> splitEntry : splitValues.entrySet()) {
            for (String ignored : splitEntry.getValue()) {
                newIdColumn.add(newIdColumn.size());
            }
        }
        return newIdColumn;
    }

    @SuppressWarnings("unchecked")
    private Column<?> createReferencingColumn(Column<?> sourceColumn, Map<Integer, String[]> splitValues, String newReferencingColumnName) {
        if (sourceColumn.getType().name().equals(IntegerType.TYPE_NAME)) {
            Column<Integer> integerColumn = (Column<Integer>) sourceColumn;
            Column<Integer> newReferencingColumn = new GenericColumn<>(newReferencingColumnName, ColumnTypeFactory.getColumnType(IntegerType.TYPE_NAME));
            for (Map.Entry<Integer, String[]> splitEntry : splitValues.entrySet()) {
                int value = integerColumn.get(splitEntry.getKey());
                for (String ignored : splitEntry.getValue()) {
                    newReferencingColumn.add(value);
                }
            }
            return newReferencingColumn;
        }
        if (sourceColumn.getType().name().equals(DoubleType.TYPE_NAME)) {
            Column<Double> doubleColumn = (Column<Double>) sourceColumn;
            Column<Double> newReferencingColumn = new GenericColumn<>(newReferencingColumnName, ColumnTypeFactory.getColumnType(DoubleType.TYPE_NAME));
            for (Map.Entry<Integer, String[]> splitEntry : splitValues.entrySet()) {
                double value = doubleColumn.get(splitEntry.getKey());
                for (String ignored : splitEntry.getValue()) {
                    newReferencingColumn.add(value);
                }
            }
            return newReferencingColumn;
        }
        Column<String> stringColumn = (Column<String>) sourceColumn;
        StringType stringType = (StringType) ColumnTypeFactory.getColumnType(StringType.TYPE_NAME);
        stringType.setMaxLength(((StringType) stringColumn.getType()).getMaxLength());
        Column<String> newReferencingColumn = new GenericColumn<>(newReferencingColumnName, stringType);
            for (Map.Entry<Integer, String[]> splitEntry : splitValues.entrySet()) {
            String value = stringColumn.get(splitEntry.getKey());
            for (String ignored : splitEntry.getValue()) {
                newReferencingColumn.add(value);
            }
        }
        return newReferencingColumn;
    }

    private Column<String> createReferencedColumn(Map<Integer, String[]> splitValues, String newReferencedColumnName) {
        Column<String> newReferencedColumn= new GenericColumn<>(newReferencedColumnName, ColumnTypeFactory.getColumnType(IntegerType.TYPE_NAME));
        for (Map.Entry<Integer, String[]> splitEntry : splitValues.entrySet()) {
            for (String splitValue : splitEntry.getValue()) {
                newReferencedColumn.add(splitValue);
            }
        }
        return newReferencedColumn;
    }

    private void validateTable(Column<?> sourceColumn, Table newTable, Column<String> newReferencedColumn) {
        ColumnValidator columnTypeValidator = ((ColumnTypeValidator.ColumnTypeValidatorBuilder) ColumnValidatorBuilderFactory.getColumnValidatorBuilder(ColumnTypeValidator.VALIDATOR_NAME))
                .column(newReferencedColumn)
                .columnType(sourceColumn.getType())
                .build();
        this.configuration.typePolicy().apply(newTable, columnTypeValidator);
        newTable.set(createColumn(newReferencedColumn, sourceColumn.getType()));
        ColumnValidator multiValueReferenceValidator = ((MultiValueReferenceValidator.MultiValueReferenceValidatorBuilder) ColumnValidatorBuilderFactory.getColumnValidatorBuilder(ANALYSIS_NAME))
                .referencingColumn(newTable.get(newReferencedColumn.getName()))
                .referencedColumn(sourceColumn)
                .build();
        this.configuration.multiValueReferencePolicy().apply(newTable, multiValueReferenceValidator);
    }

    private Column<?> createColumn(Column<String> sourceColumn, ColumnType columnType) {
        if (columnType.name().equals(IntegerType.TYPE_NAME)) {
            Column<Integer> newColumn = new GenericColumn<>(sourceColumn.getName(), ColumnTypeFactory.getColumnType(IntegerType.TYPE_NAME));
            sourceColumn.forEach(entry -> newColumn.add(entry.getKey(), Integer.valueOf(entry.getValue())));
            return newColumn;
        }
        if (columnType.name().equals(DoubleType.TYPE_NAME)) {
            Column<Double> newColumn = new GenericColumn<>(sourceColumn.getName(), ColumnTypeFactory.getColumnType(DoubleType.TYPE_NAME));
            sourceColumn.forEach(entry -> newColumn.add(entry.getKey(), Double.valueOf(entry.getValue())));
            return newColumn;
        }
        if (columnType.name().equals(StringType.TYPE_NAME)) {
            StringType stringType = (StringType) sourceColumn.getType();
            stringType.setMaxLength(sourceColumn.stream()
                    .map(entry -> entry.getValue().length())
                    .max(Integer::compare)
                    .orElse(1));
        }
        return sourceColumn;
    }

    private Constraint createUniqueConstraint(Column<?> column) {
        return ((UniqueConstraint.UniqueConstraintBuilder) ConstraintBuilderFactory.getConstraintBuilder(UniqueConstraint.CONSTRAINT_NAME))
                .column(column)
                .build();
    }

    private Constraint createReferenceConstraint(Column<?> referencingColumn, Table referencedTable, Column<?> referencedColumn) {
        return ((ReferenceConstraint.ReferenceConstraintBuilder) ConstraintBuilderFactory.getConstraintBuilder(ReferenceConstraint.CONSTRAINT_NAME))
                .referencingColumn(referencingColumn)
                .referencedTable(referencedTable)
                .referencedColumn(referencedColumn)
                .build();
    }

    public static class MultiValueReferenceAnalysisBuilder implements AnalysisBuilder {
        private final MultiValueReferenceAnalysis multiValueReferenceAnalysis;

        private MultiValueReferenceAnalysisBuilder() {
            this.multiValueReferenceAnalysis = new MultiValueReferenceAnalysis();
        }

        public MultiValueReferenceAnalysisBuilder multiValueReference(String referencingTableName, String referencingColumnName, String multiValueReferenceSeparator, String referencedTableName, String referencedColumnName, String newTableName, String newIdColumnName, String newReferencingColumnName, String newReferencedColumnName) {
            this.multiValueReferenceAnalysis.multiValueReferences.computeIfAbsent(referencingTableName, k -> new HashMap<>()).put(referencingColumnName, List.of(multiValueReferenceSeparator, referencedTableName, referencedColumnName, newTableName, newIdColumnName, newReferencingColumnName, newReferencedColumnName));
            return this;
        }

        @Override
        public MultiValueReferenceAnalysis build() {
            return this.multiValueReferenceAnalysis;
        }
    }
}
