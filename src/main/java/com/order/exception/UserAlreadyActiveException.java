package com.order.exception;

public class UserAlreadyActiveException extends RuntimeException {

    public UserAlreadyActiveException(Long id) {
        super("User with id: " + id + " is already active");
    }
}