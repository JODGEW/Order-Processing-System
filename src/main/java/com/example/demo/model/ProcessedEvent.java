package com.example.demo.model;

import jakarta.persistence.*;
import java.time.Instant;

// Tracks which events have been successfully processed by which consumer group.
//
// Why do we need this?
// Kafka guarantees at-least-once delivery, NOT exactly-once.
// If a consumer processes an event but crashes before committing the offset,
// Kafka redelivers the same event. Without this table, PaymentService would
// charge the customer twice.
//
// The unique constraint on (eventId + consumerGroup) means:
//   - PaymentService can process event "abc-123" once
//   - NotificationService can ALSO process event "abc-123" once
//   - But PaymentService can NEVER process "abc-123" a second time
@Entity
@Table(
    name = "processed_events",
    uniqueConstraints = @UniqueConstraint(columnNames = {"eventId", "consumerGroup"})
)
public class ProcessedEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // The eventId from OrderEvent or PaymentEvent
    @Column(nullable = false)
    private String eventId;

    // Which consumer group processed it — "payment-group", "notification-group", etc.
    @Column(nullable = false)
    private String consumerGroup;

    @Column(nullable = false)
    private Instant processedAt;

    public ProcessedEvent() {}

    public ProcessedEvent(String eventId, String consumerGroup) {
        this.eventId = eventId;
        this.consumerGroup = consumerGroup;
        this.processedAt = Instant.now();
    }

    // --- Getters ---

    public Long getId() { return id; }
    public String getEventId() { return eventId; }
    public String getConsumerGroup() { return consumerGroup; }
    public Instant getProcessedAt() { return processedAt; }
}
