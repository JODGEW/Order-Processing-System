package com.example.demo.service;

import com.example.demo.model.OutboxEvent;
import com.example.demo.repository.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

// This is the "relay" part of the transactional outbox pattern.
// It runs on a fixed schedule, reads unpublished events from the outbox table,
// sends them to Kafka, and marks them as published.
//
// Why a poller instead of sending immediately?
// Because the outbox row is written inside the order's transaction.
// We can't send to Kafka inside that transaction (Kafka is not a DB — there's
// no rollback if the send fails). So we decouple: write to outbox in the TX,
// publish from outbox outside the TX.
//
// What if the poller crashes after sending but before marking published?
// The event gets sent again on the next poll. That's fine — consumers are
// idempotent (Step 2.5), so duplicates are harmless.
@Service
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxPublisher(OutboxRepository outboxRepository,
                           KafkaTemplate<String, String> kafkaTemplate) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    // Runs every 500ms. Picks up unpublished outbox rows and sends to Kafka.
    @Scheduled(fixedDelay = 500)
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEvent> pending = outboxRepository.findByPublishedFalseOrderByCreatedAt();

        for (OutboxEvent event : pending) {
            try {
                // Send to Kafka: topic from the outbox row, partitionKey as the Kafka key,
                // payload (JSON string) as the value
                kafkaTemplate.send(event.getTopic(), event.getPartitionKey(), event.getPayload());

                event.setPublished(true);

                log.info("Published outbox event: type={}, aggregateId={}, topic={}",
                        event.getEventType(), event.getAggregateId(), event.getTopic());
            } catch (Exception e) {
                // If Kafka send fails, leave published=false.
                // The next poll cycle (500ms later) will retry this event.
                log.error("Failed to publish outbox event {}: {}", event.getId(), e.getMessage());
                break;  // Stop processing — maintain ordering within this batch
            }
        }
    }
}
