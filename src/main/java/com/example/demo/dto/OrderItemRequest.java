package com.example.demo.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;

// Represents a single line item in an incoming order request.
// Example JSON:  { "productName": "Widget", "quantity": 2, "unitPrice": 14.99 }
public class OrderItemRequest {

    @NotBlank(message = "productName is required")
    public String productName;

    @NotNull(message = "quantity is required")
    @Positive(message = "quantity must be positive")
    public Integer quantity;

    @NotNull(message = "unitPrice is required")
    @Positive(message = "unitPrice must be positive")
    public BigDecimal unitPrice;
}
