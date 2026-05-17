package com.order.infrastructure.config.converter;

import com.order.domain.enums.ProductSortField;
import org.jspecify.annotations.NonNull;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
public class ProductSortFieldConverter implements Converter<String, ProductSortField> {
    @Override
    public ProductSortField convert(@NonNull String source) {
        return Arrays.stream(ProductSortField.values())
                .filter(enumConstant -> enumConstant.field().equalsIgnoreCase(source))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown sort field: " + source));
    }
}
