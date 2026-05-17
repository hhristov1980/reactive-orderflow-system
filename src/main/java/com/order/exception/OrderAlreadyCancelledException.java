package com.order.exception;

public class OrderAlreadyCancelledException extends RuntimeException {
    public OrderAlreadyCancelledException(Long id) {
        super("Order with id: " + id + "is already cancelled!");
    }
}
