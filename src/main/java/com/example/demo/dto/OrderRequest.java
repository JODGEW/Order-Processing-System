package com.example.demo.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import java.util.List;

// The client sends userId + a list of items.
// totalPrice is NOT sent by the client — we calculate it server-side
// from (quantity * unitPrice) for each item. Never trust the client for pricing.
public class OrderRequest {

    @NotBlank(message = "userId is required")
    public String userId;

    @NotEmpty(message = "items must not be empty")
    @Valid
    public List<OrderItemRequest> items;

    // Client-generated UUID to prevent duplicate orders from network retries.
    // Optional — if not provided, no idempotency check is performed.
    public String idempotencyKey;
}
