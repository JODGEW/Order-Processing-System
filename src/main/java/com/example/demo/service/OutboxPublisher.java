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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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

    // How long to wait for the broker to acknowledge a single send.
    private static final long SEND_TIMEOUT_SECONDS = 10;

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
                // kafkaTemplate.send() is asynchronous: it returns a future and most
                // failures (broker down, timeout) surface on that future, not as a
                // synchronous exception. Block until the broker acknowledges the record
                // so we only mark the row published once Kafka really has it.
                kafkaTemplate.send(event.getTopic(), event.getPartitionKey(), event.getPayload())
                        .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);

                event.setPublished(true);

                log.info("Published outbox event: type={}, aggregateId={}, topic={}",
                        event.getEventType(), event.getAggregateId(), event.getTopic());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while publishing outbox event {}", event.getId());
                break;
            } catch (ExecutionException | TimeoutException | RuntimeException e) {
                // Send failed or was not acknowledged in time: leave published=false.
                // The next poll cycle (500ms later) will retry this event.
                log.error("Failed to publish outbox event {}: {}", event.getId(), e.getMessage());
                break;  // Stop processing — maintain ordering within this batch
            }
        }
    }
}
