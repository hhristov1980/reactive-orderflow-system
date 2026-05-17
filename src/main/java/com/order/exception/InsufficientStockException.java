package com.order.exception;

public class InsufficientStockException extends RuntimeException {

    public InsufficientStockException(Long productId, int requested, int available) {
        super(
                "Insufficient stock for product id: "
                        + productId
                        + ". Requested: "
                        + requested
                        + ", available: "
                        + available
        );
    }
}