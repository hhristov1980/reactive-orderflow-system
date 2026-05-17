package com.order.infrastructure.config.converter;

import com.order.domain.enums.ProductSortField;
import com.order.domain.enums.UserSortField;
import org.jspecify.annotations.NonNull;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
public class UserSortFieldConverter implements Converter<String, UserSortField> {

    @Override
    public UserSortField convert(@NonNull String source) {
        return Arrays.stream(UserSortField.values())
                .filter(enumConstant -> enumConstant.field().equalsIgnoreCase(source))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown sort field: " + source));
    }
}