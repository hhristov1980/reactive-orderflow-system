package com.order.exception;

public class AuditEventNotFoundException extends RuntimeException {

    public AuditEventNotFoundException(Long id) {
        super("Audit event with id: " + id + " was not found");
    }
}