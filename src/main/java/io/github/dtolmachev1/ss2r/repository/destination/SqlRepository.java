package io.github.dtolmachev1.ss2r.repository.destination;

import io.github.dtolmachev1.ss2r.configuration.Configuration;
import io.github.dtolmachev1.ss2r.configuration.XmlConfiguration;
import io.github.dtolmachev1.ss2r.data.column.Column;
import io.github.dtolmachev1.ss2r.data.column.ColumnType;
import io.github.dtolmachev1.ss2r.data.constraint.Constraint;
import io.github.dtolmachev1.ss2r.data.constraint.ReferenceConstraint;
import io.github.dtolmachev1.ss2r.data.constraint.UniqueConstraint;
import io.github.dtolmachev1.ss2r.data.database.Database;
import io.github.dtolmachev1.ss2r.data.table.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class SqlRepository implements DestinationRepository {
    private String databaseName;
    private final Configuration configuration;

    private SqlRepository() {
        this.configuration = XmlConfiguration.newInstance();
    }

    public static SqlRepository newInstance(String databaseName) {
        SqlRepositoryHolder.SQL_REPOSITORY.databaseName = SqlRepositoryHolder.SQL_REPOSITORY.configuration.dbmsConfiguration().url() + "/" + databaseName;
        return SqlRepositoryHolder.SQL_REPOSITORY;
    }

    @Override
    public void save(Database database) {
        try {
            Class.forName(this.configuration.dbmsConfiguration().driverClassName());
            try (Connection connection = DriverManager.getConnection(this.databaseName, this.configuration.dbmsConfiguration().username(), this.configuration.dbmsConfiguration().password())) {
                database.forEach(entry -> saveTable(connection, entry.getValue()));
                database.forEach(entry -> saveConstraints(connection, entry.getValue()));
            } catch (SQLException e) {
                throw new RuntimeException("Failed while storing into database");
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Driver class not found");
        }
    }

    private void saveTable(Connection connection, Table table) {
        try (Statement statement = connection.createStatement()) {
            statement.execute(buildCreateTableStatement(table));
            try (PreparedStatement preparedStatement = connection.prepareStatement(buildInsertStatement(table))) {
                List<Column<?>> columns = table.stream()
                        .map(Entry::getValue)
                        .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
                if (!columns.isEmpty()) {
                    columns.get(0).forEach(entry -> addInsertToStatement(preparedStatement, entry.getKey(), columns));
                }
                preparedStatement.executeBatch();
            } catch (SQLException e) {
                throw new RuntimeException("Failed while storing into database");
            }
            for (Constraint constraint : table.constraints()) {
                if (constraint.name().equals(UniqueConstraint.CONSTRAINT_NAME)) {
                    statement.execute(buildAddConstraintStatement(table, constraint));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed while storing into database");
        }
    }

    private void saveConstraints(Connection connection, Table table) {
        try (Statement statement = connection.createStatement()) {
            for (Constraint constraint : table.constraints()) {
                if (!constraint.name().equals(UniqueConstraint.CONSTRAINT_NAME)) {
                    statement.execute(buildAddConstraintStatement(table, constraint));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed while storing into database");
        }
    }

    private String buildCreateTableStatement(Table table) {
        StringBuilder statementBuilder = new StringBuilder()
                .append("CREATE TABLE IF NOT EXISTS ")
                .append(table.name())
                .append(" (");
        boolean anyFound = false;
        for (Entry<String, Column<?>> entry : table) {
            if (anyFound) {
                statementBuilder.append(", ");
            }
            anyFound = true;
            statementBuilder.append(entry.getValue().name())
                    .append(" ")
                    .append(entry.getValue().type().toString());
            if (entry.getValue().type().equals(ColumnType.STRING)) {
                statementBuilder.append("(")
                        .append(entry.getValue().maxLength())
                        .append(")");
            }
            statementBuilder.append(" NOT NULL");
        }
        statementBuilder.append(")");
        return statementBuilder.toString();
    }

    @SuppressWarnings("SpellCheckingInspection")
    private String buildAddConstraintStatement(Table table, Constraint constraint) {
        StringBuilder statementBuilder = new StringBuilder()
                .append("ALTER TABLE ONLY ")
                .append(table.name())
                .append(" ADD CONSTRAINT ")
                .append(table.name());
        if (constraint.name().equals(UniqueConstraint.CONSTRAINT_NAME)) {
            statementBuilder.append("_pkey PRIMARY KEY (")
                    .append(constraint.column().name())
                    .append(")");
        } else if (constraint.name().equals(ReferenceConstraint.CONSTRAINT_NAME)) {
            ReferenceConstraint referenceConstraint = (ReferenceConstraint) constraint;
            statementBuilder.append("_")
                    .append(referenceConstraint.column().name())
                    .append("_fkey FOREIGN KEY (")
                    .append(referenceConstraint.column().name())
                    .append(") REFERENCES ")
                    .append(referenceConstraint.referencedTable().name())
                    .append("(")
                    .append(referenceConstraint.referencedColumn().name())
                    .append(")");
        }
        return statementBuilder.toString();
    }

    private String buildInsertStatement(Table table) {
        StringBuilder statementBuilder = new StringBuilder()
                .append("INSERT INTO ")
                .append(table.name())
                .append(" (");
        boolean anyFound = false;
        for (Entry<String, Column<?>> entry : table) {
            if (anyFound) {
                statementBuilder.append(", ");
            }
            anyFound = true;
            statementBuilder.append(entry.getKey());
        }
        statementBuilder.append(") VALUES (");
        for (int i = 0; i < table.size(); i++) {
            if (i > 0) {
                statementBuilder.append(", ");
            }
            statementBuilder.append("?");
        }
        statementBuilder.append(")");
        return statementBuilder.toString();
    }

    @SuppressWarnings("unchecked")
    private void addInsertToStatement(PreparedStatement preparedStatement, Integer id, List<Column<?>> columns) {
        try {
            for (int i = 0; i < columns.size(); i++) {
                switch (columns.get(i).type()) {
                    case STRING -> preparedStatement.setString(i + 1, ((Column<String>) columns.get(i)).get(id));
                    case INTEGER -> preparedStatement.setInt(i + 1, ((Column<Integer>) columns.get(i)).get(id));
                    case DOUBLE -> preparedStatement.setDouble(i + 1, ((Column<Double>) columns.get(i)).get(id));
                }
            }
            preparedStatement.addBatch();
        } catch (SQLException e) {
            throw new RuntimeException("Failed while storing into database");
        }
    }

    private static class SqlRepositoryHolder {
        private static final SqlRepository SQL_REPOSITORY = new SqlRepository();
    }
}
