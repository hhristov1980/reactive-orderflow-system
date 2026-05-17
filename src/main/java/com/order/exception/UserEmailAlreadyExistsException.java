package com.order.exception;

public class UserEmailAlreadyExistsException extends RuntimeException {

    public UserEmailAlreadyExistsException(String email) {
        super("User with email already exists: " + email);
    }
}