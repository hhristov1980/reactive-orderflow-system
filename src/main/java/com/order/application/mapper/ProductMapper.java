package com.order.application.mapper;

import com.order.domain.dto.request.CreateProductRequest;
import com.order.domain.dto.response.ProductResponse;
import com.order.domain.entity.Product;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface ProductMapper {

    Product toEntity(CreateProductRequest request);

    ProductResponse toResponse(Product product);
}
