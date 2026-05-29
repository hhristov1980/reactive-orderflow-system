package com.order.exception;

public class OutboxEventCannotBeRetriedException extends RuntimeException {

    public OutboxEventCannotBeRetriedException(Long id, String status) {
        super("Outbox event with id: " + id + " cannot be retried from status: " + status);
    }
}