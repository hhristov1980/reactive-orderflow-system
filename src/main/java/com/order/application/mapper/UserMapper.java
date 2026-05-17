package com.order.application.mapper;

import com.order.domain.dto.request.CreateUserRequest;
import com.order.domain.dto.request.UpdateUserRequest;
import com.order.domain.dto.response.UserResponse;
import com.order.domain.entity.User;
import org.mapstruct.Mapper;
import org.mapstruct.MappingTarget;

@Mapper(componentModel = "spring")
public interface UserMapper {

    User toEntity(CreateUserRequest request);

    UserResponse toResponse(User user);

    void updateUser(
            UpdateUserRequest request,
            @MappingTarget User user
    );
}