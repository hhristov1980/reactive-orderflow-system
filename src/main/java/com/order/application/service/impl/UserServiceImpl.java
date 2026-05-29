package com.order.application.service.impl;

import com.order.application.mapper.UserMapper;
import com.order.application.service.UserService;
import com.order.domain.dto.request.CreateUserRequest;
import com.order.domain.dto.request.UpdateUserRequest;
import com.order.domain.dto.response.PagedResponse;
import com.order.domain.dto.response.UserResponse;
import com.order.domain.entity.User;
import com.order.domain.enums.SortDirection;
import com.order.domain.enums.UserRole;
import com.order.domain.enums.UserSortField;
import com.order.domain.enums.UserStatus;
import com.order.exception.*;
import com.order.infrastructure.repository.UserRepository;
import com.order.infrastructure.repository.custom.UserCustomRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class UserServiceImpl implements UserService {

    private final UserRepository repository;
    private final UserCustomRepository customRepository;
    private final UserMapper mapper;

    @Override
    public Mono<UserResponse> create(CreateUserRequest request) {
        log.info("Creating user with email {}", request.email());

        return repository.existsByEmail(request.email())
                .flatMap(exists -> {
                    if (exists) {
                        return Mono.error(
                                new UserEmailAlreadyExistsException(request.email())
                        );
                    }

//                    User user = mapper.toEntity(request);
//                    initializeAuditFields(user);

                    return repository.save(buildUser(request));
                })
                .map(mapper::toResponse);
    }

    @Override
    public Mono<UserResponse> getById(Long id) {
        log.info("Getting user with id {}", id);

        return repository.findById(id)
                .switchIfEmpty(Mono.error(new UserNotFoundException(id)))
                .map(mapper::toResponse);
    }

    @Override
    public Mono<PagedResponse<UserResponse>> getAll(
            int page,
            int size,
            UserSortField sortBy,
            SortDirection direction
    ) {
        log.info(
                "Getting all users: page {}, size {}, sortBy {}, direction {}",
                page,
                size,
                sortBy,
                direction
        );

        int validatedPage = Math.max(page, 0);
        int validatedSize = Math.clamp(size, 1, 100);

        Mono<List<UserResponse>> usersMono =
                customRepository.findAllPaged(
                                validatedPage,
                                validatedSize,
                                sortBy,
                                direction
                        )
                        .map(mapper::toResponse)
                        .collectList();

        Mono<Long> countMono =
                customRepository.countAll();

        return Mono.zip(usersMono, countMono)
                .map(tuple -> {
                    List<UserResponse> users = tuple.getT1();
                    long totalElements = tuple.getT2();
                    int totalPages = (int) Math.ceil((double) totalElements / validatedSize);

                    boolean first = validatedPage == 0;
                    boolean last = totalPages == 0 || validatedPage >= totalPages - 1;
                    boolean hasNext = validatedPage < totalPages - 1;
                    boolean hasPrevious = validatedPage > 0;
                    return new PagedResponse<>(
                            users,
                            validatedPage,
                            validatedSize,
                            totalElements,
                            totalPages,
                            first,
                            last,
                            hasNext,
                            hasPrevious
                    );
                });
    }

    @Override
    public Mono<UserResponse> update(Long id, UpdateUserRequest request) {
        log.info("Updating user with id {}", id);

        return repository.findById(id)
                .switchIfEmpty(Mono.error(new UserNotFoundException(id)))
                .flatMap(user ->
                        repository.existsByEmail(request.email())
                                .flatMap(emailExists -> {
                                    boolean emailBelongsToAnotherUser =
                                            emailExists && !user.getEmail().equals(request.email());

                                    if (emailBelongsToAnotherUser) {
                                        return Mono.error(
                                                new UserEmailAlreadyExistsException(request.email())
                                        );
                                    }

                                    mapper.updateUser(request, user);
                                    updateAuditFields(user);

                                    return repository.save(user);
                                })
                )
                .map(mapper::toResponse);
    }

    @Override
    public Mono<Void> delete(Long id) {
        log.info("Deleting user with id {}", id);

        return repository.findById(id)
                .switchIfEmpty(Mono.error(new UserNotFoundException(id)))
                .flatMap(repository::delete);
    }

    @Override
    public Mono<UserResponse> block(Long id) {
        log.info("Blocking user with id {}", id);

        return repository.findById(id)
                .switchIfEmpty(Mono.error(new UserNotFoundException(id)))
                .flatMap(user -> {
                    if (user.getStatus() == UserStatus.DELETED) {
                        return Mono.error(new UserDeletedException(id));
                    }

                    if (user.getStatus() == UserStatus.BLOCKED) {
                        return Mono.error(new UserAlreadyBlockedException(id));
                    }

                    user.setStatus(UserStatus.BLOCKED);
                    user.setUpdatedAt(OffsetDateTime.now());

                    return repository.save(user);
                })
                .map(mapper::toResponse);
    }

    @Override
    public Mono<UserResponse> activate(Long id) {
        log.info("Activating user with id {}", id);

        return repository.findById(id)
                .switchIfEmpty(Mono.error(new UserNotFoundException(id)))
                .flatMap(user -> {
                    if (user.getStatus() == UserStatus.DELETED) {
                        return Mono.error(new UserDeletedException(id));
                    }

                    if (user.getStatus() == UserStatus.ACTIVE) {
                        return Mono.error(new UserAlreadyActiveException(id));
                    }

                    user.setStatus(UserStatus.ACTIVE);
                    user.setUpdatedAt(OffsetDateTime.now());

                    return repository.save(user);
                })
                .map(mapper::toResponse);
    }

    private void initializeAuditFields(User user) {
        OffsetDateTime now = OffsetDateTime.now();

        user.setCreatedAt(now);
        user.setUpdatedAt(now);
    }

    private void updateAuditFields(User user) {
        user.setUpdatedAt(OffsetDateTime.now());
    }

    private User buildUser(CreateUserRequest request) {
        OffsetDateTime now = OffsetDateTime.now();

        return User.builder()
                .name(request.name())
                .email(request.email())
                .role(UserRole.CUSTOMER)
                .status(UserStatus.ACTIVE)
                .emailVerified(false)
                .createdAt(now)
                .updatedAt(now)
                .build();
    }
}