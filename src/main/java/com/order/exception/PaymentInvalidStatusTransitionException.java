package com.order.exception;

public class PaymentInvalidStatusTransitionException extends RuntimeException {

    public PaymentInvalidStatusTransitionException(
            Long paymentId,
            String currentStatus,
            String targetStatus
    ) {
        super(
                "Payment with id: "
                        + paymentId
                        + " cannot transition from "
                        + currentStatus
                        + " to "
                        + targetStatus
        );
    }
}