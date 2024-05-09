package io.github.dtolmachev1.data.column;

public class DoubleType implements ColumnType {
    public static final String TYPE_NAME = "double precision";

    private DoubleType() {
    }

    public static DoubleType newInstance() {
        return DoubleTypeHolder.DOUBLE_TYPE;
    }

    @Override
    public String name() {
        return TYPE_NAME;
    }

    private static class DoubleTypeHolder {
        private static final DoubleType DOUBLE_TYPE = new DoubleType();
    }
}
