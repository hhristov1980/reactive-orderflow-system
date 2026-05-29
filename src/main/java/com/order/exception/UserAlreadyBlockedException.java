package com.order.exception;

public class UserAlreadyBlockedException extends RuntimeException {

    public UserAlreadyBlockedException(Long id) {
        super("User with id: " + id + " is already blocked");
    }
}