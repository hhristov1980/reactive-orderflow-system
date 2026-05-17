package com.order.infrastructure.config.converter;

import com.order.domain.enums.SortDirection;
import com.order.domain.enums.UserSortField;
import org.jspecify.annotations.NonNull;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
public class SortDirectionConverter implements Converter<String, SortDirection> {

    @Override
    public SortDirection convert(@NonNull String source) {
        return Arrays.stream(SortDirection.values())
                .filter(enumConstant -> enumConstant.field().equalsIgnoreCase(source))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown sort field: " + source));
    }
}