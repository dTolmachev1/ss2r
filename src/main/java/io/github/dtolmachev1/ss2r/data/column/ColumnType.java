package io.github.dtolmachev1.ss2r.data.column;

public enum ColumnType {
    STRING {
        @Override
        public String toString() {
            return "character varying";
        }
    },
    INTEGER {
        @Override
        public String toString() {
            return "integer";
        }
    },
    DOUBLE {
        @Override
        public String toString() {
            return "double precision";
        }
    }
}
