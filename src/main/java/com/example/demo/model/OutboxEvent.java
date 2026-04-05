package com.example.demo.model;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

// The outbox table stores events that need to be published to Kafka.
//
// Why not just call kafkaTemplate.send() directly?
// Because saving an order and sending a Kafka message are TWO separate operations:
//   1. orderRepository.save(order)   → writes to Postgres
//   2. kafkaTemplate.send(event)     → sends to Kafka
//
// If the app crashes between step 1 and step 2, the order exists in the DB
// but the event was never published. Payment never happens. Silent data loss.
//
// The outbox pattern fixes this: write the event to a DB table in the SAME
// transaction as the order. A background poller reads unpublished rows and
// sends them to Kafka. If the poller crashes, it just retries on next tick —
// the row is still there.
@Entity
@Table(name = "outbox")
public class OutboxEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    // Which entity this event belongs to (e.g., the order ID)
    @Column(nullable = false)
    private String aggregateId;

    // e.g., "ORDER_CREATED"
    @Column(nullable = false)
    private String eventType;

    // The Kafka partition key — we use userId so all events for one user
    // land on the same partition (explained in Step 2.3)
    @Column(nullable = false)
    private String partitionKey;

    // The full event serialized as JSON
    @Column(nullable = false, columnDefinition = "TEXT")
    private String payload;

    // Which Kafka topic to publish to
    @Column(nullable = false)
    private String topic;

    @Column(nullable = false, updatable = false)
    private Instant createdAt;

    // false = not yet sent to Kafka, true = successfully published
    @Column(nullable = false)
    private boolean published = false;

    @PrePersist
    protected void onCreate() {
        this.createdAt = Instant.now();
    }

    // --- Getters and Setters ---

    public UUID getId() { return id; }

    public String getAggregateId() { return aggregateId; }
    public void setAggregateId(String aggregateId) { this.aggregateId = aggregateId; }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }

    public String getPartitionKey() { return partitionKey; }
    public void setPartitionKey(String partitionKey) { this.partitionKey = partitionKey; }

    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }

    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }

    public Instant getCreatedAt() { return createdAt; }

    public boolean isPublished() { return published; }
    public void setPublished(boolean published) { this.published = published; }
}
