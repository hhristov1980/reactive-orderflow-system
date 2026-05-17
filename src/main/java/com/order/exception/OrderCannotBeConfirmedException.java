package com.order.exception;

public class OrderCannotBeConfirmedException extends RuntimeException {
    public OrderCannotBeConfirmedException(Long id, String status) {
        super("Order with id: " + id + " cannot be confirmed from status: " + status);
    }
}
