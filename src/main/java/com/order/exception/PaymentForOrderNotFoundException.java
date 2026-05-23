package com.order.exception;

public class PaymentForOrderNotFoundException extends RuntimeException {

    public PaymentForOrderNotFoundException(Long orderId) {
        super("Payment not found for order id: " + orderId);
    }
}