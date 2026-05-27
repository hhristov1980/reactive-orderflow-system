package com.order.domain.entity;

import com.order.domain.enums.OutboxStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.OffsetDateTime;

@Table("outbox_events")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OutboxEvent {

    @Id
    private Long id;
    private String aggregateType;
    private Long aggregateId;
    private String eventType;
    private String topic;
    private String eventKey;
    private String payload;
    private OutboxStatus status;
    private Integer retryCount;
    private String lastError;
    private OffsetDateTime createdAt;
    private  OffsetDateTime updatedAt;
    private OffsetDateTime publishedAt;
}