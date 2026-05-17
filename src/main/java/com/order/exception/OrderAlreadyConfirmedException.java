package com.order.exception;

public class OrderAlreadyConfirmedException extends RuntimeException {
    public OrderAlreadyConfirmedException(Long id) {
        super("Order with id: " + id + " is already confirmed!");
    }
}
