package com.order.application.mapper;

import com.order.domain.dto.response.InventoryResponse;
import com.order.domain.entity.Inventory;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface InventoryMapper {

    InventoryResponse toResponse(Inventory inventory);
}