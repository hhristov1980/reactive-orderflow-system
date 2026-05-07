package com.order.exception;

import java.time.OffsetDateTime;

public record ErrorResponse(
        String message,
        OffsetDateTime timestamp
) {
}
