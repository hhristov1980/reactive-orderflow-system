package com.order.infrastructure.config.converter;

import com.order.domain.enums.InventorySortField;
import org.jspecify.annotations.NonNull;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
public class InventorySortFieldConverter implements Converter<String, InventorySortField> {

    @Override
    public InventorySortField convert(@NonNull String source) {
        return Arrays.stream(InventorySortField.values())
                .filter(enumConstant -> enumConstant.field().equalsIgnoreCase(source))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown sort field: " + source));
    }
}