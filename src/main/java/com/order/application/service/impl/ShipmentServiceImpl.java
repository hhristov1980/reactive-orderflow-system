package com.order.application.service.impl;

import com.order.application.mapper.ShipmentMapper;
import com.order.application.service.OutboxService;
import com.order.application.service.ShipmentService;
import com.order.domain.dto.response.ShipmentResponse;
import com.order.domain.entity.Shipment;
import com.order.domain.enums.ShipmentStatus;
import com.order.domain.event.*;
import com.order.exception.ShipmentAlreadyExistsException;
import com.order.exception.ShipmentForOrderNotFoundException;
import com.order.exception.ShipmentInvalidStatusTransitionException;
import com.order.exception.ShipmentNotFoundException;
import com.order.infrastructure.config.properties.OrderKafkaProperties;
import com.order.infrastructure.repository.ShipmentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class ShipmentServiceImpl implements ShipmentService {

    private static final String DEFAULT_CARRIER = "DHL";
    private static final String AGGREGATE_TYPE_SHIPMENT = "SHIPMENT";

    private static final String EVENT_TYPE_SHIPMENT_CREATED = "SHIPMENT_CREATED";
    private static final String EVENT_TYPE_SHIPMENT_SHIPPED = "SHIPMENT_SHIPPED";
    private static final String EVENT_TYPE_SHIPMENT_DELIVERED = "SHIPMENT_DELIVERED";

    private final ShipmentRepository repository;
    private final ShipmentMapper mapper;
    private final OutboxService outboxService;
    private final OrderKafkaProperties kafkaProperties;
    private final TransactionalOperator transactionalOperator;

    @Override
    public Mono<ShipmentResponse> createFromPaymentCompleted(PaymentCompletedEvent event) {
        log.info("Creating shipment for paid orderId={}", event.orderId());

        Mono<ShipmentResponse> flow =
                repository.existsByOrderId(event.orderId())
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
                                outboxService.saveEvent(
                                                AGGREGATE_TYPE_SHIPMENT,
                                                savedShipment.getId(),
                                                EVENT_TYPE_SHIPMENT_CREATED,
                                                kafkaProperties.getTopics().getShipmentCreated(),
                                                savedShipment.getOrderId().toString(),
                                                toShipmentCreatedEvent(savedShipment)
                                        )
                                        .thenReturn(savedShipment)
                        )
                        .map(mapper::toResponse);

        return transactionalOperator.transactional(flow);
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

        Mono<ShipmentResponse> flow =
                repository.findById(id)
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
                                outboxService.saveEvent(
                                                AGGREGATE_TYPE_SHIPMENT,
                                                savedShipment.getId(),
                                                EVENT_TYPE_SHIPMENT_SHIPPED,
                                                kafkaProperties.getTopics().getShipmentShipped(),
                                                savedShipment.getOrderId().toString(),
                                                toShipmentShippedEvent(savedShipment)
                                        )
                                        .thenReturn(savedShipment)
                        )
                        .map(mapper::toResponse);

        return transactionalOperator.transactional(flow);
    }

    @Override
    public Mono<ShipmentResponse> markAsDelivered(Long id) {
        log.info("Marking shipment as delivered. shipmentId={}", id);

        Mono<ShipmentResponse> flow =
                repository.findById(id)
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
                                outboxService.saveEvent(
                                                AGGREGATE_TYPE_SHIPMENT,
                                                savedShipment.getId(),
                                                EVENT_TYPE_SHIPMENT_DELIVERED,
                                                kafkaProperties.getTopics().getShipmentDelivered(),
                                                savedShipment.getOrderId().toString(),
                                                toShipmentDeliveredEvent(savedShipment)
                                        )
                                        .thenReturn(savedShipment)
                        )
                        .map(mapper::toResponse);

        return transactionalOperator.transactional(flow);
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