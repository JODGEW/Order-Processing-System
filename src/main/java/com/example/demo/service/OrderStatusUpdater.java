package com.example.demo.service;

import com.example.demo.event.PaymentEvent;
import com.example.demo.model.Order;
import com.example.demo.model.OrderStatus;
import com.example.demo.model.ProcessedEvent;
import com.example.demo.repository.OrderRepository;
import com.example.demo.repository.ProcessedEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

// Listens to payment-events and updates the order's status.
//
// Why a separate class instead of putting this in OrderService?
// Separation of concerns: OrderService handles the REST API / command side.
// This class handles the event-driven / reactive side.
// They both use OrderRepository, but they're triggered differently
// (HTTP request vs Kafka message).
@Service
public class OrderStatusUpdater {

    private static final Logger log = LoggerFactory.getLogger(OrderStatusUpdater.class);
    private static final String GROUP_ID = "order-status-group";

    private final OrderRepository orderRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final ObjectMapper objectMapper;

    public OrderStatusUpdater(OrderRepository orderRepository,
                              ProcessedEventRepository processedEventRepository,
                              ObjectMapper objectMapper) {
        this.orderRepository = orderRepository;
        this.processedEventRepository = processedEventRepository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "payment-events", groupId = GROUP_ID)
    @Transactional
    public void handlePaymentEvent(String payload) {
        try {
            PaymentEvent event = objectMapper.readValue(payload, PaymentEvent.class);

            // --- Idempotency guard ---
            if (processedEventRepository.existsByEventIdAndConsumerGroup(event.getEventId(), GROUP_ID)) {
                log.info("Duplicate payment event {}, skipping", event.getEventId());
                return;
            }

            // Look up the order
            Order order = orderRepository.findById(UUID.fromString(event.getOrderId()))
                    .orElseThrow(() -> new RuntimeException("Order not found: " + event.getOrderId()));

            // Map event type to the target status
            OrderStatus newStatus = switch (event.getEventType()) {
                case "PAYMENT_COMPLETED" -> OrderStatus.PAID;
                case "PAYMENT_FAILED" -> OrderStatus.PAYMENT_FAILED;
                default -> throw new RuntimeException("Unknown payment event type: " + event.getEventType());
            };

            // Transition through the state machine.
            // This will throw IllegalStateException if the transition is invalid
            // (e.g., trying to set a CANCELLED order to PAID).
            //
            // When Hibernate flushes this change, the @Version check kicks in.
            // If another thread modified this order concurrently,
            // OptimisticLockException is thrown → Kafka retries the message.
            order.transitionTo(newStatus);

            log.info("Order {} status updated: {} → {}",
                    event.getOrderId(), order.getStatus(), newStatus);

            // Record that we processed this event (same transaction)
            processedEventRepository.save(new ProcessedEvent(event.getEventId(), GROUP_ID));

        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize payment event: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
