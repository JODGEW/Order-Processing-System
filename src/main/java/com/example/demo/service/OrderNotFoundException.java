package com.example.demo.service;

import java.util.UUID;

// Thrown when an order lookup by ID finds nothing.
// A dedicated type lets the HTTP layer map "not found" to 404 without
// swallowing every other RuntimeException (DB down, bugs, ...) as 404 too.
public class OrderNotFoundException extends RuntimeException {

    public OrderNotFoundException(UUID id) {
        super("Order not found: " + id);
    }
}
