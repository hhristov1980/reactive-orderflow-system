package com.order.application.mapper;

import com.order.domain.dto.response.PaymentResponse;
import com.order.domain.entity.Payment;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface PaymentMapper {

    PaymentResponse toResponse(Payment payment);
}