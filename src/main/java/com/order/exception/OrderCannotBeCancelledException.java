package com.order.exception;

public class OrderCannotBeCancelledException extends RuntimeException {

    public OrderCannotBeCancelledException(Long id, String status) {
        super("Order with id: " + id + " cannot be cancelled from status: " + status);
    }
}