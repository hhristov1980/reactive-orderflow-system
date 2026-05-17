package com.order.infrastructure.config.converter;

import com.order.domain.enums.OrderSortField;
import org.jspecify.annotations.NonNull;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
public class OrderSortFieldConverter implements Converter<String, OrderSortField> {

    @Override
    public OrderSortField convert(@NonNull String source) {
        return Arrays.stream(OrderSortField.values())
                .filter(enumConstant -> enumConstant.field().equalsIgnoreCase(source))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown sort field: " + source));
    }
}