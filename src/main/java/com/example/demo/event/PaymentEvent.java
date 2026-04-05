package com.example.demo.event;

import java.time.Instant;
import java.util.UUID;

// Published by PaymentService after it processes an order.
// Consumed by NotificationService and the order status updater.
public class PaymentEvent {

    private String eventId;
    private String eventType;   // "PAYMENT_COMPLETED" or "PAYMENT_FAILED"
    private String orderId;
    private Instant timestamp;

    public PaymentEvent() {}

    public static PaymentEvent completed(String orderId) {
        PaymentEvent event = new PaymentEvent();
        event.eventId = UUID.randomUUID().toString();
        event.eventType = "PAYMENT_COMPLETED";
        event.orderId = orderId;
        event.timestamp = Instant.now();
        return event;
    }

    public static PaymentEvent failed(String orderId) {
        PaymentEvent event = new PaymentEvent();
        event.eventId = UUID.randomUUID().toString();
        event.eventType = "PAYMENT_FAILED";
        event.orderId = orderId;
        event.timestamp = Instant.now();
        return event;
    }

    // --- Getters and Setters ---

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }
}
