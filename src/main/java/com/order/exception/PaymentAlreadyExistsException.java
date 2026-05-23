package com.order.exception;

public class PaymentAlreadyExistsException extends RuntimeException {

    public PaymentAlreadyExistsException(Long orderId) {
        super("Payment already exists for order id: " + orderId);
    }
}