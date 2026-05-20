package com.order.exception;

public class ShipmentForOrderNotFoundException extends RuntimeException {

    public ShipmentForOrderNotFoundException(Long orderId) {
        super("Shipment not found for order id: " + orderId);
    }
}