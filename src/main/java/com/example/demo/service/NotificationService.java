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
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

// Listens to BOTH topics — order-events and payment-events.
// Simulates sending notifications (email/SMS) by logging.
//
// groupId = "notification-group" — separate from "payment-group".
// This means NotificationService gets its OWN copy of every event.
// Kafka delivers each event to one consumer per group, so:
//   - PaymentService (payment-group) gets the order event
//   - NotificationService (notification-group) ALSO gets the same order event
// Both process independently. This is the fan-out pattern.
@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);
    private static final String GROUP_ID = "notification-group";

    private final ObjectMapper objectMapper;
    private final ProcessedEventRepository processedEventRepository;

    public NotificationService(ObjectMapper objectMapper,
                               ProcessedEventRepository processedEventRepository) {
        this.objectMapper = objectMapper;
        this.processedEventRepository = processedEventRepository;
    }

    @KafkaListener(topics = "order-events", groupId = GROUP_ID)
    @Transactional
    public void handleOrderEvent(String payload) {
        try {
            OrderEvent event = objectMapper.readValue(payload, OrderEvent.class);

            if (processedEventRepository.existsByEventIdAndConsumerGroup(event.getEventId(), GROUP_ID)) {
                log.info("Duplicate order event {}, skipping", event.getEventId());
                return;
            }

            log.info("[NOTIFICATION] Order {} — {} | userId={}, total={}",
                    event.getOrderId(), event.getEventType(),
                    event.getUserId(), event.getTotalPrice());

            processedEventRepository.save(new ProcessedEvent(event.getEventId(), GROUP_ID));
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize order event: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @KafkaListener(topics = "payment-events", groupId = GROUP_ID)
    @Transactional
    public void handlePaymentEvent(String payload) {
        try {
            PaymentEvent event = objectMapper.readValue(payload, PaymentEvent.class);

            if (processedEventRepository.existsByEventIdAndConsumerGroup(event.getEventId(), GROUP_ID)) {
                log.info("Duplicate payment event {}, skipping", event.getEventId());
                return;
            }

            log.info("[NOTIFICATION] Payment {} — order={}",
                    event.getEventType(), event.getOrderId());

            processedEventRepository.save(new ProcessedEvent(event.getEventId(), GROUP_ID));
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize payment event: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
