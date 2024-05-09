package io.github.dtolmachev1.inference.validator;

import io.github.dtolmachev1.data.column.Column;

public class MultiValueReferenceValidator extends ReferenceConstraintValidator {
    public static final String VALIDATOR_NAME = "multi-value-reference";

    public MultiValueReferenceValidator(Column<?> referencingColumn, Column<?> referencedColumn) {
        super(referencingColumn, referencedColumn);
    }

    @Override
    public String name() {
        return VALIDATOR_NAME;
    }

    public static MultiValueReferenceValidatorBuilder builder() {
        return new MultiValueReferenceValidatorBuilder();
    }

    public static final class MultiValueReferenceValidatorBuilder extends ReferenceConstraintValidatorBuilder {
        private MultiValueReferenceValidatorBuilder() {
            super();
        }
    }
}
