package com.order.exception;

public class UserBlockedException extends RuntimeException {

    public UserBlockedException(Long id) {
        super("User with id: " + id + " is blocked");
    }
}