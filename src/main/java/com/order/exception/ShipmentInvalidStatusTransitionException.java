package com.order.exception;

public class ShipmentInvalidStatusTransitionException extends RuntimeException {

    public ShipmentInvalidStatusTransitionException(
            Long shipmentId,
            String currentStatus,
            String targetStatus
    ) {
        super(
                "Shipment with id: "
                        + shipmentId
                        + " cannot transition from "
                        + currentStatus
                        + " to "
                        + targetStatus
        );
    }
}