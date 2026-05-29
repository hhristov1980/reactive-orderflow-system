package com.order.infrastructure.repository;

import com.order.domain.entity.Product;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;

public interface ProductRepository
        extends ReactiveCrudRepository<Product, Long> {
}
