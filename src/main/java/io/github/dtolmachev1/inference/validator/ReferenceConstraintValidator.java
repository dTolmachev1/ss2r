package io.github.dtolmachev1.inference.validator;

import io.github.dtolmachev1.data.column.Column;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ReferenceConstraintValidator implements ColumnValidator {
    public static final String VALIDATOR_NAME = "reference-constraint";
    private Column<?> referencingColumn;
    private Set<Object> referencedValues;

    protected ReferenceConstraintValidator() {
    }

    public ReferenceConstraintValidator(Column<?> referencingColumn, Column<?> referencedColumn) {
        this.referencingColumn = referencingColumn;
        initializeReferencedValues(referencedColumn);
    }

    @Override
    public String name() {
        return VALIDATOR_NAME;
    }

    @Override
    public boolean isValid(Integer id) {
        return this.referencedValues.contains(this.referencingColumn.get(id));
    }

    public static ReferenceConstraintValidatorBuilder builder() {
        return new ReferenceConstraintValidatorBuilder();
    }

    private void initializeReferencedValues(Column<?> referencedColumn) {
        this.referencedValues = referencedColumn.stream().map(Map.Entry::getValue).collect(HashSet::new, HashSet::add, HashSet::addAll);
    }

    public static class ReferenceConstraintValidatorBuilder implements ColumnValidatorBuilder {
        private final ReferenceConstraintValidator referenceConstraintValidator;

        protected ReferenceConstraintValidatorBuilder() {
            this.referenceConstraintValidator = new ReferenceConstraintValidator();
        }

        public ReferenceConstraintValidatorBuilder referencingColumn(Column<?> referencingColumn) {
            this.referenceConstraintValidator.referencingColumn = referencingColumn;
            return this;
        }

        public ReferenceConstraintValidatorBuilder referencedColumn(Column<?> referencedColumn) {
            this.referenceConstraintValidator.initializeReferencedValues(referencedColumn);
            return this;
        }

        @Override
        public ReferenceConstraintValidator build() {
            return this.referenceConstraintValidator;
        }
    }
}
