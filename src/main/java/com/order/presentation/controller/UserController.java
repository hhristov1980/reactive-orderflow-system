package com.order.presentation.controller;

import com.order.application.service.UserService;
import com.order.domain.dto.request.CreateUserRequest;
import com.order.domain.dto.request.UpdateUserRequest;
import com.order.domain.dto.response.PagedResponse;
import com.order.domain.dto.response.UserResponse;
import com.order.domain.enums.SortDirection;
import com.order.domain.enums.UserSortField;
import io.swagger.v3.oas.annotations.Operation;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Positive;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/v1/users")
@RequiredArgsConstructor
@Validated
public class UserController {

    private final UserService service;

    @Operation(summary = "Create user")
    @PostMapping
    public Mono<ResponseEntity<UserResponse>> create(
            @Valid @RequestBody CreateUserRequest request
    ) {
        return service.create(request)
                .map(response ->
                        ResponseEntity
                                .status(HttpStatus.CREATED)
                                .body(response)
                );
    }

    @Operation(summary = "Get user by id")
    @GetMapping("/{id}")
    public Mono<ResponseEntity<UserResponse>> getById(
            @PathVariable @Positive Long id
    ) {
        return service.getById(id)
                .map(ResponseEntity::ok);
    }

    @Operation(summary = "Get all users")
    @GetMapping
    public Mono<ResponseEntity<PagedResponse<UserResponse>>> getAll(

            @RequestParam(defaultValue = "0")
            int page,
            @RequestParam(defaultValue = "20")
            int size,
            @RequestParam(defaultValue = "ID")
            UserSortField sortBy,
            @RequestParam(defaultValue = "ASC")
            SortDirection direction
    ) {
        return service.getAll(
                        page,
                        size,
                        sortBy,
                        direction
                )
                .map(ResponseEntity::ok);
    }

    @Operation(summary = "Update user")
    @PutMapping("/{id}")
    public Mono<ResponseEntity<UserResponse>> update(
            @PathVariable @Positive Long id,
            @Valid @RequestBody UpdateUserRequest request
    ) {
        return service.update(id, request)
                .map(ResponseEntity::ok);
    }

    @Operation(summary = "Delete user")
    @DeleteMapping("/{id}")
    public Mono<ResponseEntity<Void>> delete(
            @PathVariable @Positive Long id
    ) {
        return service.delete(id)
                .then(Mono.just(ResponseEntity.noContent().build()));
    }
}