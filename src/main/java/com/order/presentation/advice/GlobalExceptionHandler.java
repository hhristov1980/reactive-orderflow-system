package com.order.presentation.advice;

import com.order.domain.dto.error.ErrorResponse;
import com.order.exception.ProductNotFoundException;
import org.springframework.context.MessageSourceResolvable;
import org.springframework.context.support.DefaultMessageSourceResolvable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.validation.BindingResult;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.bind.support.WebExchangeBindException;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebInputException;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.stream.Collectors;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handle(
            Exception ex,
            ServerHttpRequest request
    ) {

        ErrorResponse response = new ErrorResponse(
                HttpStatus.INTERNAL_SERVER_ERROR.value(),
                HttpStatus.INTERNAL_SERVER_ERROR.name(),
                ex.getMessage(),
                request.getPath().value(),
                OffsetDateTime.now()
        );

        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(response);
    }

    @ExceptionHandler(ProductNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleProductNotFound(
            ProductNotFoundException ex,
            ServerHttpRequest request
    ) {

        ErrorResponse response = new ErrorResponse(
                HttpStatus.NOT_FOUND.value(),
                HttpStatus.NOT_FOUND.name(),
                ex.getMessage(),
                request.getPath().value(),
                OffsetDateTime.now()
        );

        return ResponseEntity
                .status(HttpStatus.NOT_FOUND)
                .body(response);
    }

    @ExceptionHandler({
            WebExchangeBindException.class,
            ResponseStatusException.class,
            HandlerMethodValidationException.class})
    public ResponseEntity<ErrorResponse> handleValidation(
            Exception ex,
            ServerHttpRequest request
    ) {
        String message = "Validation failure";

        if (ex instanceof WebExchangeBindException bindEx) {
            message = extractBindingErrors(bindEx.getBindingResult());
        }
        else if (ex instanceof HandlerMethodValidationException validationEx) {
            message = validationEx.getValueResults().stream()
                    .map(result -> {
                        String paramName = result.getMethodParameter().getParameterName();
                        String errorMsg = result.getResolvableErrors().stream()
                                .map(MessageSourceResolvable::getDefaultMessage)
                                .collect(Collectors.joining(", "));

                        return (paramName != null ? paramName : "parameter") + ": " + errorMsg;
                    })
                    .collect(Collectors.joining("; "));
        }
        else if (ex instanceof ResponseStatusException statusEx) {
            message = statusEx.getReason();
        }

        ErrorResponse response = new ErrorResponse(
                HttpStatus.BAD_REQUEST.value(),
                HttpStatus.BAD_REQUEST.getReasonPhrase(),
                message,
                request.getPath().value(),
                OffsetDateTime.now()
        );

        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }

    @ExceptionHandler(ServerWebInputException.class)
    public ResponseEntity<ErrorResponse> handleServerWebInput(
            ServerWebInputException ex,
            ServerHttpRequest request
    ) {
        String message = "Invalid input";

        if (ex instanceof WebExchangeBindException bindEx) {
            message = bindEx.getBindingResult()
                    .getFieldErrors()
                    .stream()
                    .map(f -> f.getField() + ": " + f.getDefaultMessage())
                    .collect(Collectors.joining(", "));
        }
        else {
            Throwable rootCause = ex.getMostSpecificCause();
            if (rootCause instanceof IllegalArgumentException && rootCause.getMessage().contains("No enum constant")) {
                String fullMessage = rootCause.getMessage();
                String invalidValue = fullMessage.substring(fullMessage.lastIndexOf('.') + 1);

                message = String.format("Invalid value '%s' for parameter. Please provide a valid sort field.", invalidValue);
            } else {
                message = ex.getReason();
            }
        }

        ErrorResponse response = new ErrorResponse(
                HttpStatus.BAD_REQUEST.value(),
                "INVALID_PARAMETER_TYPE",
                message,
                request.getPath().value(),
                OffsetDateTime.now()
        );

        return ResponseEntity.badRequest().body(response);
    }

    private String extractBindingErrors(BindingResult bindingResult) {
        return bindingResult.getFieldErrors()
                .stream()
                .map(error -> error.getField() + ": " + error.getDefaultMessage())
                .collect(Collectors.joining(", "));
    }



}
