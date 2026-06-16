package com.order.infrastructure.scheduler;

import com.order.application.service.PaymentService;
import com.order.infrastructure.config.properties.UnpaidPaymentSchedulerProperties;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class UnpaidPaymentSchedulerTest {

    private final PaymentService paymentService = mock(PaymentService.class);
    private final UnpaidPaymentSchedulerProperties properties =
            new UnpaidPaymentSchedulerProperties();

    private final UnpaidPaymentScheduler scheduler =
            new UnpaidPaymentScheduler(paymentService, properties);

    @Test
    void shouldNotRunWhenSchedulerIsDisabled() {
        properties.setEnabled(false);
        properties.setExpirationDays(2);
        properties.setFixedDelayMs(60_000L);

        scheduler.expireUnpaidPayments();

        verify(paymentService, never()).expireOverduePayments(any());
    }

    @Test
    void shouldRunWhenSchedulerIsEnabled() {
        properties.setEnabled(true);
        properties.setExpirationDays(2);
        properties.setFixedDelayMs(60_000L);

        when(paymentService.expireOverduePayments(any()))
                .thenReturn(Mono.just(3));

        scheduler.expireUnpaidPayments();

        verify(paymentService).expireOverduePayments(any(OffsetDateTime.class));
    }

    @Test
    void shouldUseCutoffBasedOnExpirationDays() {
        properties.setEnabled(true);
        properties.setExpirationDays(3);
        properties.setFixedDelayMs(60_000L);

        AtomicReference<OffsetDateTime> cutoffRef = new AtomicReference<>();

        when(paymentService.expireOverduePayments(any()))
                .thenAnswer(invocation -> {
                    cutoffRef.set(invocation.getArgument(0));
                    return Mono.just(1);
                });

        OffsetDateTime beforeRun = OffsetDateTime.now().minusDays(3);

        scheduler.expireUnpaidPayments();

        OffsetDateTime afterRun = OffsetDateTime.now().minusDays(3);

        assertThat(cutoffRef.get()).isNotNull();
        assertThat(cutoffRef.get()).isAfterOrEqualTo(beforeRun.minusSeconds(1));
        assertThat(cutoffRef.get()).isBeforeOrEqualTo(afterRun.plusSeconds(1));
    }

    @Test
    void shouldSubscribeAndHandleServiceErrorWithoutThrowing() {
        properties.setEnabled(true);
        properties.setExpirationDays(2);
        properties.setFixedDelayMs(60_000L);

        when(paymentService.expireOverduePayments(any()))
                .thenReturn(Mono.error(new RuntimeException("Payment expiration failed")));

        scheduler.expireUnpaidPayments();

        verify(paymentService).expireOverduePayments(any(OffsetDateTime.class));
    }
}