package io.github.dtolmachev1.inference.validator;

public interface TableValidator {
    String name();

    boolean isValid(String columnName);

    interface TableValidatorBuilder {
        TableValidator build();
    }
}
