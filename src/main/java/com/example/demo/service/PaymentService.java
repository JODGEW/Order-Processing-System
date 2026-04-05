package com.example.demo.service;

import com.example.demo.event.OrderEvent;
import com.example.demo.event.PaymentEvent;
import com.example.demo.model.ProcessedEvent;
import com.example.demo.repository.ProcessedEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Random;

// Consumes ORDER_CREATED events from the "order-events" topic.
// Simulates payment processing (80% success, 20% failure for demo purposes).
// Publishes the result to "payment-events" topic.
//
// groupId = "payment-group" means:
//   - All instances of PaymentService share this consumer group
//   - Kafka assigns partitions across instances — each event is processed by ONE instance
//   - If you scale to 3 PaymentService instances, Kafka splits the load across them
@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);
    private static final String GROUP_ID = "payment-group";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final ProcessedEventRepository processedEventRepository;
    private final Random random = new Random();

    public PaymentService(KafkaTemplate<String, String> kafkaTemplate,
                          ObjectMapper objectMapper,
                          ProcessedEventRepository processedEventRepository) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.processedEventRepository = processedEventRepository;
    }

    // @Transactional ensures the idempotency check + business logic + processed_events insert
    // all happen in ONE database transaction. If any part fails, everything rolls back.
    // The event will be redelivered by Kafka and retried cleanly.
    @KafkaListener(topics = "order-events", groupId = GROUP_ID)
    @Transactional
    public void handleOrderEvent(String payload) {
        try {
            OrderEvent event = objectMapper.readValue(payload, OrderEvent.class);

            if (!"ORDER_CREATED".equals(event.getEventType())) {
                return;
            }

            // --- Idempotency guard ---
            // Check if we already processed this exact event in this consumer group.
            // If yes, skip. This handles Kafka redelivery after crashes/rebalances.
            if (processedEventRepository.existsByEventIdAndConsumerGroup(event.getEventId(), GROUP_ID)) {
                log.info("Duplicate event {}, skipping", event.getEventId());
                return;
            }

            log.info("Processing payment for order={}, userId={}, amount={}",
                    event.getOrderId(), event.getUserId(), event.getTotalPrice());

            // Simulate payment processing — 80% success rate
            boolean success = random.nextInt(100) < 80;

            PaymentEvent result;
            if (success) {
                result = PaymentEvent.completed(event.getOrderId());
                log.info("Payment COMPLETED for order={}", event.getOrderId());
            } else {
                result = PaymentEvent.failed(event.getOrderId());
                log.warn("Payment FAILED for order={}", event.getOrderId());
            }

            // Publish result to payment-events topic
            kafkaTemplate.send("payment-events", event.getUserId(), toJson(result));

            // --- Record that we processed this event ---
            // This insert is part of the same @Transactional.
            // If the business logic above succeeded but this insert fails → rollback all.
            // Event gets redelivered → retried cleanly.
            processedEventRepository.save(new ProcessedEvent(event.getEventId(), GROUP_ID));

        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize order event: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize payment event", e);
        }
    }
}
