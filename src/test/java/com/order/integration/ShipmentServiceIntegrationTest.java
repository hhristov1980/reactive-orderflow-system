package com.order.integration;

import com.order.application.mapper.ShipmentMapper;
import com.order.application.service.OutboxService;
import com.order.application.service.impl.ShipmentServiceImpl;
import com.order.domain.dto.response.ShipmentResponse;
import com.order.domain.entity.Shipment;
import com.order.domain.enums.ShipmentStatus;
import com.order.domain.event.PaymentCompletedEvent;
import com.order.exception.ShipmentAlreadyExistsException;
import com.order.exception.ShipmentInvalidStatusTransitionException;
import com.order.infrastructure.config.properties.OrderKafkaProperties;
import com.order.infrastructure.repository.ShipmentRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.data.r2dbc.test.autoconfigure.DataR2dbcTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@DataR2dbcTest
@Import(ShipmentServiceImpl.class)
@EnableConfigurationProperties(OrderKafkaProperties.class)
@TestPropertySource(properties = {
        "orderflow.kafka.topics.shipment-created=shipment.created",
        "orderflow.kafka.topics.shipment-shipped=shipment.shipped",
        "orderflow.kafka.topics.shipment-delivered=shipment.delivered"
})
class ShipmentServiceIntegrationTest extends AbstractPostgresTestcontainersTest {

    @Autowired
    private ShipmentServiceImpl shipmentService;

    @Autowired
    private ShipmentRepository shipmentRepository;

    @MockitoBean
    private ShipmentMapper shipmentMapper;

    @MockitoBean
    private OutboxService outboxService;

    @Test
    void shouldCreateShipmentFromPaymentCompletedAndSaveOutboxEvent() {
        ShipmentResponse response = mock(ShipmentResponse.class);

        when(shipmentMapper.toResponse(any(Shipment.class)))
                .thenReturn(response);

        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.empty());

        AtomicReference<Long> shipmentIdRef = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(shipmentService.createFromPaymentCompleted(paymentCompletedEvent(101L)))
                )
                .expectNext(response)
                .verifyComplete();

        StepVerifier.create(shipmentRepository.findByOrderId(101L))
                .expectNextMatches(shipment -> {
                    shipmentIdRef.set(shipment.getId());

                    return shipment.getOrderId().equals(101L)
                            && shipment.getStatus() == ShipmentStatus.CREATED
                            && shipment.getTrackingNumber() != null
                            && shipment.getTrackingNumber().startsWith("TRK-")
                            && shipment.getCarrier().equals("DHL")
                            && shipment.getCreatedAt() != null
                            && shipment.getUpdatedAt() != null
                            && shipment.getShippedAt() == null
                            && shipment.getDeliveredAt() == null;
                })
                .verifyComplete();

        verify(outboxService).saveEvent(
                eq("SHIPMENT"),
                eq(shipmentIdRef.get()),
                eq("SHIPMENT_CREATED"),
                eq("shipment.created"),
                eq("101"),
                any()
        );
    }

    @Test
    void shouldRejectCreatingShipmentWhenShipmentAlreadyExistsForOrder() {
        ShipmentResponse response = mock(ShipmentResponse.class);

        when(shipmentMapper.toResponse(any(Shipment.class)))
                .thenReturn(response);

        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.empty());

        StepVerifier.create(
                        cleanDatabase()
                                .then(shipmentRepository.save(newShipment(
                                        201L,
                                        ShipmentStatus.CREATED
                                )))
                                .then(shipmentService.createFromPaymentCompleted(paymentCompletedEvent(201L)))
                )
                .expectError(ShipmentAlreadyExistsException.class)
                .verify();

        StepVerifier.create(shipmentRepository.findByOrderId(201L))
                .expectNextMatches(shipment ->
                        shipment.getOrderId().equals(201L)
                                && shipment.getStatus() == ShipmentStatus.CREATED
                )
                .verifyComplete();

        verify(outboxService, never()).saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        );
    }

    @Test
    void shouldMarkCreatedShipmentAsShippedAndSaveOutboxEvent() {
        ShipmentResponse response = mock(ShipmentResponse.class);

        when(shipmentMapper.toResponse(any(Shipment.class)))
                .thenReturn(response);

        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.empty());

        AtomicReference<Long> shipmentIdRef = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(shipmentRepository.save(newShipment(
                                        301L,
                                        ShipmentStatus.CREATED
                                )))
                                .doOnNext(savedShipment -> shipmentIdRef.set(savedShipment.getId()))
                                .flatMap(savedShipment ->
                                        shipmentService.markAsShipped(savedShipment.getId())
                                )
                )
                .expectNext(response)
                .verifyComplete();

        StepVerifier.create(shipmentRepository.findById(shipmentIdRef.get()))
                .expectNextMatches(shipment ->
                        shipment.getStatus() == ShipmentStatus.SHIPPED
                                && shipment.getShippedAt() != null
                                && shipment.getDeliveredAt() == null
                )
                .verifyComplete();

        verify(outboxService).saveEvent(
                eq("SHIPMENT"),
                eq(shipmentIdRef.get()),
                eq("SHIPMENT_SHIPPED"),
                eq("shipment.shipped"),
                eq("301"),
                any()
        );
    }

    @Test
    void shouldMarkShippedShipmentAsDeliveredAndSaveOutboxEvent() {
        ShipmentResponse response = mock(ShipmentResponse.class);

        when(shipmentMapper.toResponse(any(Shipment.class)))
                .thenReturn(response);

        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.empty());

        AtomicReference<Long> shipmentIdRef = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(shipmentRepository.save(newShippedShipment(401L)))
                                .doOnNext(savedShipment -> shipmentIdRef.set(savedShipment.getId()))
                                .flatMap(savedShipment ->
                                        shipmentService.markAsDelivered(savedShipment.getId())
                                )
                )
                .expectNext(response)
                .verifyComplete();

        StepVerifier.create(shipmentRepository.findById(shipmentIdRef.get()))
                .expectNextMatches(shipment ->
                        shipment.getStatus() == ShipmentStatus.DELIVERED
                                && shipment.getShippedAt() != null
                                && shipment.getDeliveredAt() != null
                )
                .verifyComplete();

        verify(outboxService).saveEvent(
                eq("SHIPMENT"),
                eq(shipmentIdRef.get()),
                eq("SHIPMENT_DELIVERED"),
                eq("shipment.delivered"),
                eq("401"),
                any()
        );
    }

    @Test
    void shouldRejectMarkingCreatedShipmentAsDelivered() {
        ShipmentResponse response = mock(ShipmentResponse.class);

        when(shipmentMapper.toResponse(any(Shipment.class)))
                .thenReturn(response);

        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.empty());

        AtomicReference<Long> shipmentIdRef = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(shipmentRepository.save(newShipment(
                                        501L,
                                        ShipmentStatus.CREATED
                                )))
                                .doOnNext(savedShipment -> shipmentIdRef.set(savedShipment.getId()))
                                .flatMap(savedShipment ->
                                        shipmentService.markAsDelivered(savedShipment.getId())
                                )
                )
                .expectError(ShipmentInvalidStatusTransitionException.class)
                .verify();

        StepVerifier.create(shipmentRepository.findById(shipmentIdRef.get()))
                .expectNextMatches(shipment ->
                        shipment.getStatus() == ShipmentStatus.CREATED
                                && shipment.getDeliveredAt() == null
                )
                .verifyComplete();

        verify(outboxService, never()).saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        );
    }

    @Test
    void shouldRejectMarkingDeliveredShipmentAsShipped() {
        ShipmentResponse response = mock(ShipmentResponse.class);

        when(shipmentMapper.toResponse(any(Shipment.class)))
                .thenReturn(response);

        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.empty());

        AtomicReference<Long> shipmentIdRef = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(shipmentRepository.save(newDeliveredShipment(601L)))
                                .doOnNext(savedShipment -> shipmentIdRef.set(savedShipment.getId()))
                                .flatMap(savedShipment ->
                                        shipmentService.markAsShipped(savedShipment.getId())
                                )
                )
                .expectError(ShipmentInvalidStatusTransitionException.class)
                .verify();

        StepVerifier.create(shipmentRepository.findById(shipmentIdRef.get()))
                .expectNextMatches(shipment ->
                        shipment.getStatus() == ShipmentStatus.DELIVERED
                                && shipment.getShippedAt() != null
                                && shipment.getDeliveredAt() != null
                )
                .verifyComplete();

        verify(outboxService, never()).saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        );
    }

    @Test
    void shouldRollbackShipmentCreationWhenOutboxSaveFails() {
        ShipmentResponse response = mock(ShipmentResponse.class);

        when(shipmentMapper.toResponse(any(Shipment.class)))
                .thenReturn(response);

        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.error(new RuntimeException("Outbox failure")));

        StepVerifier.create(
                        cleanDatabase()
                                .then(shipmentService.createFromPaymentCompleted(paymentCompletedEvent(701L)))
                )
                .expectError(RuntimeException.class)
                .verify();

        StepVerifier.create(shipmentRepository.findAll().collectList())
                .expectNextMatches(java.util.List::isEmpty)
                .verifyComplete();

        verify(outboxService).saveEvent(
                anyString(),
                anyLong(),
                eq("SHIPMENT_CREATED"),
                anyString(),
                anyString(),
                any()
        );
    }

    @Test
    void shouldRollbackMarkAsShippedWhenOutboxSaveFails() {
        ShipmentResponse response = mock(ShipmentResponse.class);

        when(shipmentMapper.toResponse(any(Shipment.class)))
                .thenReturn(response);

        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.error(new RuntimeException("Outbox failure")));

        AtomicReference<Long> shipmentIdRef = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(shipmentRepository.save(newShipment(
                                        801L,
                                        ShipmentStatus.CREATED
                                )))
                                .doOnNext(savedShipment -> shipmentIdRef.set(savedShipment.getId()))
                                .flatMap(savedShipment ->
                                        shipmentService.markAsShipped(savedShipment.getId())
                                )
                )
                .expectError(RuntimeException.class)
                .verify();

        StepVerifier.create(shipmentRepository.findById(shipmentIdRef.get()))
                .expectNextMatches(shipment ->
                        shipment.getStatus() == ShipmentStatus.CREATED
                                && shipment.getShippedAt() == null
                )
                .verifyComplete();

        verify(outboxService).saveEvent(
                anyString(),
                eq(shipmentIdRef.get()),
                eq("SHIPMENT_SHIPPED"),
                anyString(),
                anyString(),
                any()
        );
    }

    @Test
    void shouldRollbackMarkAsDeliveredWhenOutboxSaveFails() {
        ShipmentResponse response = mock(ShipmentResponse.class);

        when(shipmentMapper.toResponse(any(Shipment.class)))
                .thenReturn(response);

        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.error(new RuntimeException("Outbox failure")));

        AtomicReference<Long> shipmentIdRef = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(shipmentRepository.save(newShippedShipment(901L)))
                                .doOnNext(savedShipment -> shipmentIdRef.set(savedShipment.getId()))
                                .flatMap(savedShipment ->
                                        shipmentService.markAsDelivered(savedShipment.getId())
                                )
                )
                .expectError(RuntimeException.class)
                .verify();

        StepVerifier.create(shipmentRepository.findById(shipmentIdRef.get()))
                .expectNextMatches(shipment ->
                        shipment.getStatus() == ShipmentStatus.SHIPPED
                                && shipment.getShippedAt() != null
                                && shipment.getDeliveredAt() == null
                )
                .verifyComplete();

        verify(outboxService).saveEvent(
                anyString(),
                eq(shipmentIdRef.get()),
                eq("SHIPMENT_DELIVERED"),
                anyString(),
                anyString(),
                any()
        );
    }

    private Mono<Void> cleanDatabase() {
        return shipmentRepository.deleteAll();
    }

    private static PaymentCompletedEvent paymentCompletedEvent(Long orderId) {
        return new PaymentCompletedEvent(
                1001L,
                orderId,
                new BigDecimal("49.90"),
                "TXN-" + orderId,
                OffsetDateTime.now()
        );
    }

    private static Shipment newShipment(
            Long orderId,
            ShipmentStatus status
    ) {
        OffsetDateTime now = OffsetDateTime.now();

        return Shipment.builder()
                .orderId(orderId)
                .status(status)
                .trackingNumber("TRK-" + orderId)
                .carrier("DHL")
                .createdAt(now)
                .updatedAt(now)
                .shippedAt(null)
                .deliveredAt(null)
                .build();
    }

    private static Shipment newShippedShipment(Long orderId) {
        OffsetDateTime now = OffsetDateTime.now();
        OffsetDateTime shippedAt = now.minusMinutes(10);

        return Shipment.builder()
                .orderId(orderId)
                .status(ShipmentStatus.SHIPPED)
                .trackingNumber("TRK-" + orderId)
                .carrier("DHL")
                .createdAt(now.minusMinutes(20))
                .updatedAt(shippedAt)
                .shippedAt(shippedAt)
                .deliveredAt(null)
                .build();
    }

    private static Shipment newDeliveredShipment(Long orderId) {
        OffsetDateTime now = OffsetDateTime.now();
        OffsetDateTime shippedAt = now.minusMinutes(20);
        OffsetDateTime deliveredAt = now.minusMinutes(5);

        return Shipment.builder()
                .orderId(orderId)
                .status(ShipmentStatus.DELIVERED)
                .trackingNumber("TRK-" + orderId)
                .carrier("DHL")
                .createdAt(now.minusMinutes(30))
                .updatedAt(deliveredAt)
                .shippedAt(shippedAt)
                .deliveredAt(deliveredAt)
                .build();
    }
}