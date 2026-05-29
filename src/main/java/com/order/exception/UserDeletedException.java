package com.order.exception;

public class UserDeletedException extends RuntimeException {

    public UserDeletedException(Long id) {
        super("User with id: " + id + " is deleted");
    }
}