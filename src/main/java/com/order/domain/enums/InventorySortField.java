package com.order.domain.enums;

public enum InventorySortField {

    ID("id"),
    PRODUCT_ID("productId"),
    AVAILABLE_QUANTITY("availableQuantity"),
    RESERVED_QUANTITY("reservedQuantity"),
    CREATED_AT("createdAt"),
    UPDATED_AT("updatedAt");

    private final String field;

    InventorySortField(String field) {
        this.field = field;
    }

    public String field() {
        return field;
    }

}