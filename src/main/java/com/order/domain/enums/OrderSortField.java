package com.order.domain.enums;

public enum OrderSortField {

    ID("id"),
    USER_ID("userId"),
    STATUS("status"),
    TOTAL_AMOUNT("totalAmount"),
    CREATED_AT("createdAt"),
    UPDATED_AT("updatedAt");

    private final String field;

    OrderSortField(String field) {
        this.field = field;
    }

    public String field() {
        return field;
    }
}