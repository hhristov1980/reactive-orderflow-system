package com.order.exception;

public class DuplicateOrderItemException extends RuntimeException {

  public DuplicateOrderItemException(Long productId) {
    super("Duplicate product id in order items: " + productId);
  }
}