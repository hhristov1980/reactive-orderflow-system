package com.order.application.mapper;

import com.order.domain.dto.request.CreateProductRequest;
import com.order.domain.dto.request.UpdateProductRequest;
import com.order.domain.dto.response.ProductResponse;
import com.order.domain.entity.Product;
import org.mapstruct.Mapper;
import org.mapstruct.MappingTarget;

@Mapper(componentModel = "spring")
public interface ProductMapper {

    Product toEntity(CreateProductRequest request);

    ProductResponse toResponse(Product product);

    void updateProduct(
            UpdateProductRequest request,
            @MappingTarget Product product
    );
}
