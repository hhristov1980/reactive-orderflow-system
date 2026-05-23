package com.order.domain.entity;

import com.order.domain.enums.PaymentStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

@Table("payments")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Payment {

    @Id
    private Long id;
    private Long orderId;
    private PaymentStatus status;
    private BigDecimal amount;
    private String provider;
    private String transactionId;
    private String failureReason;
    private OffsetDateTime createdAt;
    private OffsetDateTime updatedAt;
    private OffsetDateTime paidAt;
    private OffsetDateTime failedAt;
    private OffsetDateTime expiredAt;
}