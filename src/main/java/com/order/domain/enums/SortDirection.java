package com.order.domain.enums;

public enum SortDirection {
    ASC("asc"),
    DESC("desc");

    private final String field;

    SortDirection(String field) {
        this.field = field;
    }

    public String field() {
        return field;
    }
}
