package com.example.demo.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

// Represents an event that something happened to an order.
// This is what gets serialized to JSON and sent to Kafka's "order-events" topic.
public class OrderEvent {

    // Unique ID for THIS event — used by consumers for idempotency (Step 2.5).
    // Not the same as the order ID. One order can produce many events
    // (ORDER_CREATED, then later ORDER_CANCELLED, etc.)
    private String eventId;

    private String eventType;   // "ORDER_CREATED"
    private String orderId;
    private String userId;
    private BigDecimal totalPrice;
    private Instant timestamp;

    // No-arg constructor required for JSON deserialization (Jackson needs it)
    public OrderEvent() {}

    public static OrderEvent orderCreated(UUID orderId, String userId, BigDecimal totalPrice) {
        OrderEvent event = new OrderEvent();
        event.eventId = UUID.randomUUID().toString();
        event.eventType = "ORDER_CREATED";
        event.orderId = orderId.toString();
        event.userId = userId;
        event.totalPrice = totalPrice;
        event.timestamp = Instant.now();
        return event;
    }

    // --- Getters and Setters (Jackson needs both for serialization/deserialization) ---

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public BigDecimal getTotalPrice() { return totalPrice; }
    public void setTotalPrice(BigDecimal totalPrice) { this.totalPrice = totalPrice; }

    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }
}
