package com.order.domain.enums;

import java.util.Arrays;

public enum UserSortField {

    ID("id"),
    EMAIL("email"),
    NAME("name"),
    CREATED_AT("createdAt"),
    UPDATED_AT("updatedAt");

    private final String field;

    UserSortField(String field) {
        this.field = field;
    }

    public String field() {
        return field;
    }

}