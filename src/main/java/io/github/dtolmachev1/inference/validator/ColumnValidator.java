package io.github.dtolmachev1.inference.validator;

public interface ColumnValidator {
    String name();

    boolean isValid(Integer id);

    interface ColumnValidatorBuilder {
        ColumnValidator build();
    }
}
