package io.github.dtolmachev1.inference.validator;

import io.github.dtolmachev1.data.column.Column;
import io.github.dtolmachev1.data.column.ColumnType;
import io.github.dtolmachev1.data.column.DoubleType;
import io.github.dtolmachev1.data.column.IntegerType;
import io.github.dtolmachev1.data.column.StringType;

public class ColumnTypeValidator implements ColumnValidator {
    public static final String VALIDATOR_NAME = "column-type";
    private Column<String> column;
    private ColumnType columnType;

    private ColumnTypeValidator() {
    }

    public ColumnTypeValidator(Column<String> column, ColumnType columnType) {
        this.column = column;
        this.columnType = columnType;
    }

    public String name() {
        return VALIDATOR_NAME;
    }

    @Override
    public boolean isValid(Integer id) {
        try {
            switch (this.columnType.name()) {
                case StringType.TYPE_NAME: return true;
                case IntegerType.TYPE_NAME: Integer.valueOf(column.get(id)); break;
                case DoubleType.TYPE_NAME: Double.valueOf(column.get(id)); break;
                default: return false;
            }
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }

    public static ColumnTypeValidatorBuilder builder() {
        return new ColumnTypeValidatorBuilder();
    }

    public static class ColumnTypeValidatorBuilder implements ColumnValidatorBuilder {
        private final ColumnTypeValidator columnTypeValidator;

        private ColumnTypeValidatorBuilder() {
            this.columnTypeValidator = new ColumnTypeValidator();
        }

        public ColumnTypeValidatorBuilder column(Column<String> column) {
            this.columnTypeValidator.column = column;
            return this;
        }

        public ColumnTypeValidatorBuilder columnType(ColumnType columnType) {
            this.columnTypeValidator.columnType = columnType;
            return this;
        }

        @Override
        public ColumnTypeValidator build() {
            return this.columnTypeValidator;
        }
    }
}
