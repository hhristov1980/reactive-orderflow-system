package com.order.application.mapper;

import com.order.domain.dto.response.ShipmentResponse;
import com.order.domain.entity.Shipment;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface ShipmentMapper {

    ShipmentResponse toResponse(Shipment shipment);
}