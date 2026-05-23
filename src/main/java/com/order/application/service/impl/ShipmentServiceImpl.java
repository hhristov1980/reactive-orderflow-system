package com.order.application.service.impl;

import com.order.application.mapper.ShipmentMapper;
import com.order.application.service.ShipmentService;
import com.order.domain.dto.response.ShipmentResponse;
import com.order.domain.entity.Shipment;
import com.order.domain.enums.ShipmentStatus;
import com.order.domain.event.*;
import com.order.exception.ShipmentAlreadyExistsException;
import com.order.exception.ShipmentForOrderNotFoundException;
import com.order.exception.ShipmentInvalidStatusTransitionException;
import com.order.exception.ShipmentNotFoundException;
import com.order.infrastructure.messaging.kafka.ShipmentEventProducer;
import com.order.infrastructure.repository.ShipmentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class ShipmentServiceImpl implements ShipmentService {

    private static final String DEFAULT_CARRIER = "DHL";

    private final ShipmentRepository repository;
    private final ShipmentMapper mapper;
    private final ShipmentEventProducer producer;

    @Override
    public Mono<ShipmentResponse> createFromPaymentCompleted(PaymentCompletedEvent event) {
        log.info("Creating shipment for paid orderId={}", event.orderId());

        return repository.existsByOrderId(event.orderId())
                .flatMap(exists -> {
                    if (exists) {
                        return Mono.error(
                                new ShipmentAlreadyExistsException(event.orderId())
                        );
                    }

                    Shipment shipment = buildShipment(event.orderId());

                    return repository.save(shipment);
                })
                .flatMap(savedShipment ->
                        producer.publishShipmentCreated(
                                        toShipmentCreatedEvent(savedShipment)
                                )
                                .thenReturn(savedShipment)
                )
                .map(mapper::toResponse);
    }

    @Override
    public Mono<ShipmentResponse> getById(Long id) {
        return repository.findById(id)
                .switchIfEmpty(Mono.error(new ShipmentNotFoundException(id)))
                .map(mapper::toResponse);
    }

    @Override
    public Mono<ShipmentResponse> getByOrderId(Long orderId) {
        return repository.findByOrderId(orderId)
                .switchIfEmpty(
                        Mono.error(
                                new ShipmentForOrderNotFoundException(orderId)
                        )
                )
                .map(mapper::toResponse);
    }

    @Override
    public Mono<ShipmentResponse> markAsShipped(Long id) {
        log.info("Marking shipment as shipped. shipmentId={}", id);

        return repository.findById(id)
                .switchIfEmpty(Mono.error(new ShipmentNotFoundException(id)))
                .flatMap(shipment -> {
                    if (shipment.getStatus() != ShipmentStatus.CREATED) {
                        return Mono.error(
                                new ShipmentInvalidStatusTransitionException(
                                        id,
                                        shipment.getStatus().name(),
                                        ShipmentStatus.SHIPPED.name()
                                )
                        );
                    }

                    shipment.setStatus(ShipmentStatus.SHIPPED);
                    shipment.setShippedAt(OffsetDateTime.now());
                    shipment.setUpdatedAt(OffsetDateTime.now());

                    return repository.save(shipment);
                })
                .flatMap(savedShipment ->
                        producer.publishShipmentShipped(
                                        toShipmentShippedEvent(savedShipment)
                                )
                                .thenReturn(savedShipment)
                )
                .map(mapper::toResponse);
    }

    @Override
    public Mono<ShipmentResponse> markAsDelivered(Long id) {
        log.info("Marking shipment as delivered. shipmentId={}", id);

        return repository.findById(id)
                .switchIfEmpty(Mono.error(new ShipmentNotFoundException(id)))
                .flatMap(shipment -> {
                    if (shipment.getStatus() != ShipmentStatus.SHIPPED) {
                        return Mono.error(
                                new ShipmentInvalidStatusTransitionException(
                                        id,
                                        shipment.getStatus().name(),
                                        ShipmentStatus.DELIVERED.name()
                                )
                        );
                    }

                    shipment.setStatus(ShipmentStatus.DELIVERED);
                    shipment.setDeliveredAt(OffsetDateTime.now());
                    shipment.setUpdatedAt(OffsetDateTime.now());

                    return repository.save(shipment);
                })
                .flatMap(savedShipment ->
                        producer.publishShipmentDelivered(
                                        toShipmentDeliveredEvent(savedShipment)
                                )
                                .thenReturn(savedShipment)
                )
                .map(mapper::toResponse);
    }

    private Shipment buildShipment(Long orderId) {
        OffsetDateTime now = OffsetDateTime.now();

        return Shipment.builder()
                .orderId(orderId)
                .status(ShipmentStatus.CREATED)
                .carrier(DEFAULT_CARRIER)
                .trackingNumber(generateTrackingNumber())
                .createdAt(now)
                .updatedAt(now)
                .build();
    }

    private String generateTrackingNumber() {
        return "TRK-" + UUID.randomUUID()
                .toString()
                .replace("-", "")
                .substring(0, 12)
                .toUpperCase();
    }

    private ShipmentCreatedEvent toShipmentCreatedEvent(Shipment shipment) {
        return new ShipmentCreatedEvent(
                shipment.getId(),
                shipment.getOrderId(),
                shipment.getTrackingNumber(),
                shipment.getCarrier(),
                shipment.getCreatedAt()
        );
    }

    private ShipmentShippedEvent toShipmentShippedEvent(Shipment shipment) {
        return new ShipmentShippedEvent(
                shipment.getId(),
                shipment.getOrderId(),
                shipment.getTrackingNumber(),
                shipment.getShippedAt()
        );
    }

    private ShipmentDeliveredEvent toShipmentDeliveredEvent(Shipment shipment) {
        return new ShipmentDeliveredEvent(
                shipment.getId(),
                shipment.getOrderId(),
                shipment.getTrackingNumber(),
                shipment.getDeliveredAt()
        );
    }
}