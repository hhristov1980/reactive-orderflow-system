package com.order.domain.entity;

import com.order.domain.enums.InventoryReservationStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.OffsetDateTime;

@Table("inventory_reservations")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class InventoryReservation {

    @Id
    private Long id;
    private Long orderId;
    private Long productId;
    private Integer quantity;
    private InventoryReservationStatus status;
    private OffsetDateTime createdAt;
    private OffsetDateTime updatedAt;
}
