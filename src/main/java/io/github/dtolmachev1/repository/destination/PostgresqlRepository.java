package io.github.dtolmachev1.repository.destination;

import io.github.dtolmachev1.configuration.Configuration;
import io.github.dtolmachev1.configuration.XmlConfiguration;
import io.github.dtolmachev1.data.column.Column;
import io.github.dtolmachev1.data.column.DoubleType;
import io.github.dtolmachev1.data.column.IntegerType;
import io.github.dtolmachev1.data.column.StringType;
import io.github.dtolmachev1.data.database.Database;
import io.github.dtolmachev1.data.table.Constraint;
import io.github.dtolmachev1.data.table.ReferenceConstraint;
import io.github.dtolmachev1.data.table.Table;
import io.github.dtolmachev1.data.table.UniqueConstraint;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PostgresqlRepository implements DestinationRepository {
    public static final String REPOSITORY_NAME = "postgres";
    private final Configuration.DbmsConfiguration configuration;

    private PostgresqlRepository() {
        this.configuration = XmlConfiguration.newInstance().dbmsConfiguration();
    }

    public static PostgresqlRepository newInstance() {
        return PostgresRepositoryHolder.POSTGRES_REPOSITORY;
    }

    @Override
    public void save(Database database) {
        try {
            Class.forName(this.configuration.driverClassName());
            try (Connection connection = DriverManager.getConnection(this.configuration.url() + "/" + database.getName(), this.configuration.username(), this.configuration.password())) {
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
                        .map(Map.Entry::getValue)
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
                .append(table.getName())
                .append(" (");
        boolean anyFound = false;
        for (Map.Entry<String, Column<?>> entry : table) {
            if (anyFound) {
                statementBuilder.append(", ");
            }
            anyFound = true;
            statementBuilder.append(entry.getValue().getName())
                    .append(" ")
                    .append(entry.getValue().getType().name());
            if (entry.getValue().getType().name().equals(StringType.TYPE_NAME)) {
                statementBuilder.append("(")
                        .append(((StringType) entry.getValue().getType()).getMaxLength())
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
                .append(table.getName())
                .append(" ADD CONSTRAINT ")
                .append(table.getName());
        if (constraint.name().equals(UniqueConstraint.CONSTRAINT_NAME)) {
            UniqueConstraint uniqueConstraint = (UniqueConstraint) constraint;
            statementBuilder.append("_pkey PRIMARY KEY (")
                    .append(uniqueConstraint.column().getName())
                    .append(")");
        } else if (constraint.name().equals(ReferenceConstraint.CONSTRAINT_NAME)) {
            ReferenceConstraint referenceConstraint = (ReferenceConstraint) constraint;
            statementBuilder.append("_")
                    .append(referenceConstraint.referencingColumn().getName())
                    .append("_fkey FOREIGN KEY (")
                    .append(referenceConstraint.referencingColumn().getName())
                    .append(") REFERENCES ")
                    .append(referenceConstraint.referencedTable().getName())
                    .append("(")
                    .append(referenceConstraint.referencedColumn().getName())
                    .append(")");
        }
        return statementBuilder.toString();
    }

    private String buildInsertStatement(Table table) {
        StringBuilder statementBuilder = new StringBuilder()
                .append("INSERT INTO ")
                .append(table.getName())
                .append(" (");
        boolean anyFound = false;
        for (Map.Entry<String, Column<?>> entry : table) {
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
                switch (columns.get(i).getType().name()) {
                    case StringType.TYPE_NAME -> preparedStatement.setString(i + 1, ((Column<String>) columns.get(i)).get(id));
                    case IntegerType.TYPE_NAME -> preparedStatement.setInt(i + 1, ((Column<Integer>) columns.get(i)).get(id));
                    case DoubleType.TYPE_NAME -> preparedStatement.setDouble(i + 1, ((Column<Double>) columns.get(i)).get(id));
                }
            }
            preparedStatement.addBatch();
        } catch (SQLException e) {
            throw new RuntimeException("Failed while storing into database");
        }
    }

    private static class PostgresRepositoryHolder {
        private static final PostgresqlRepository POSTGRES_REPOSITORY = new PostgresqlRepository();
    }
}
