package com.order.domain.enums;

public enum ProductSortField {

    ID("id"),
    NAME("name"),
    PRICE("price"),
    STOCK("stock"),
    CREATED_AT("createdAt");

    private final String field;

    ProductSortField(String field) {
        this.field = field;
    }

    public String field() {
        return field;
    }
}