package com.order.infrastructure.repository;

import com.order.domain.entity.User;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Mono;

public interface UserRepository extends ReactiveCrudRepository<User, Long> {

    Mono<Boolean> existsByEmail(String email);
}