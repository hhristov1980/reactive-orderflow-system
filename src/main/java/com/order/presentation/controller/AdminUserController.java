package com.order.presentation.controller.admin;

import com.order.application.service.UserService;
import com.order.domain.dto.response.UserResponse;
import io.swagger.v3.oas.annotations.Operation;
import jakarta.validation.constraints.Positive;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/v1/admin/users")
@RequiredArgsConstructor
@Validated
public class AdminUserController {

    private final UserService userService;

    @Operation(summary = "Block user")
    @PatchMapping("/{id}/block")
    public Mono<ResponseEntity<UserResponse>> block(
            @PathVariable @Positive Long id
    ) {
        return userService.block(id)
                .map(ResponseEntity::ok);
    }

    @Operation(summary = "Activate user")
    @PatchMapping("/{id}/activate")
    public Mono<ResponseEntity<UserResponse>> activate(
            @PathVariable @Positive Long id
    ) {
        return userService.activate(id)
                .map(ResponseEntity::ok);
    }
}