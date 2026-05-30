package com.order.domain.enums;

import org.jspecify.annotations.NonNull;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
public class StringToProductSortFieldConverter implements Converter<String, ProductSortField> {
    @Override
    public ProductSortField convert(@NonNull String source) {
        return Arrays.stream(ProductSortField.values())
                .filter(enumConstant -> enumConstant.field().equalsIgnoreCase(source))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown sort field: " + source));
    }
}
