package io.github.dtolmachev1.data.column;

public class IntegerType implements ColumnType {
    public static final String TYPE_NAME = "integer";

    private IntegerType() {
    }

    public static IntegerType newInstance() {
        return IntegerTypeHolder.INTEGER_TYPE;
    }

    @Override
    public String name() {
        return TYPE_NAME;
    }

    private static class IntegerTypeHolder {
        private static final IntegerType INTEGER_TYPE = new IntegerType();
    }
}
