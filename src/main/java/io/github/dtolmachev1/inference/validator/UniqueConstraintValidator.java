package io.github.dtolmachev1.inference.validator;

import io.github.dtolmachev1.data.column.Column;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class UniqueConstraintValidator implements ColumnValidator {
    public static final String VALIDATOR_NAME = "unique-constraint";
    private Column<?> column;
    private Map<Object, Integer> valuesCount;

    private UniqueConstraintValidator() {
    }

    public UniqueConstraintValidator(Column<?> column) {
        this.column = column;
        countValues();
    }

    @Override
    public String name() {
        return VALIDATOR_NAME;
    }

    @Override
    public boolean isValid(Integer id) {
        return this.valuesCount.get(this.column.get(id)).equals(1);
    }

    public static UniqueConstraintValidatorBuilder builder() {
        return new UniqueConstraintValidatorBuilder();
    }

    private void countValues() {
        this.valuesCount = this.column.stream()
                .map(Map.Entry::getValue)
                .collect(Collectors.toMap(value -> value, value -> 1, Integer::sum, HashMap::new));
    }

    public static class UniqueConstraintValidatorBuilder implements ColumnValidatorBuilder {
        private final UniqueConstraintValidator uniqueConstraintValidator;

        private UniqueConstraintValidatorBuilder() {
            this.uniqueConstraintValidator = new UniqueConstraintValidator();
        }

        public UniqueConstraintValidatorBuilder column(Column<?> column) {
            this.uniqueConstraintValidator.column = column;
            this.uniqueConstraintValidator.countValues();
            return this;
        }

        @Override
        public UniqueConstraintValidator build() {
            return this.uniqueConstraintValidator;
        }
    }
}
