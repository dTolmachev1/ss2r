package io.github.dtolmachev1.data.column;

public class StringType implements ColumnType {
    public static final String TYPE_NAME = "character varying";
    private int maxLength;

    public StringType() {
        this.maxLength = 0;
    }

    @Override
    public String name() {
        return TYPE_NAME;
    }

    public int getMaxLength() {
        return this.maxLength;
    }

    public void setMaxLength(int maxLength) {
        this.maxLength = maxLength;
    }
}
