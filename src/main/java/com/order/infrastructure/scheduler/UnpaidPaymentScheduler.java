package com.order.infrastructure.scheduler;

import com.order.application.service.PaymentService;
import com.order.infrastructure.config.properties.UnpaidPaymentSchedulerProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;

@Component
@RequiredArgsConstructor
@Slf4j
public class UnpaidPaymentScheduler {

    private final PaymentService paymentService;
    private final UnpaidPaymentSchedulerProperties properties;

    @Scheduled(
            fixedDelayString = "${orderflow.scheduler.unpaid-payments.fixed-delay-ms}"
    )
    public void expireUnpaidPayments() {
        if (!properties.isEnabled()) {
            log.debug("Unpaid payment scheduler is disabled");
            return;
        }

        OffsetDateTime cutoff =
                OffsetDateTime.now()
                        .minusDays(properties.getExpirationDays());

        log.info(
                "Running unpaid payment expiration job. cutoff={}",
                cutoff
        );

        paymentService.expireOverduePayments(cutoff)
                .doOnNext(count ->
                        log.info(
                                "Unpaid payment expiration job completed. expiredCount={}",
                                count
                        )
                )
                .doOnError(error ->
                        log.error(
                                "Unpaid payment expiration job failed",
                                error
                        )
                )
                .subscribe();
    }
}