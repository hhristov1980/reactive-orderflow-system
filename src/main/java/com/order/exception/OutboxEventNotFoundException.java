package com.order.exception;

public class OutboxEventNotFoundException extends RuntimeException {

    public OutboxEventNotFoundException(Long id) {
        super("Outbox event with id: " + id + " was not found");
    }
}