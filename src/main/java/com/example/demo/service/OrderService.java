package com.example.demo.service;

import com.example.demo.dto.OrderItemRequest;
import com.example.demo.dto.OrderRequest;
import com.example.demo.dto.OrderResponse;
import com.example.demo.event.OrderEvent;
import com.example.demo.model.Order;
import com.example.demo.model.OrderItem;
import com.example.demo.model.OrderStatus;
import com.example.demo.model.OutboxEvent;
import com.example.demo.repository.OrderRepository;
import com.example.demo.repository.OutboxRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public OrderService(OrderRepository orderRepository,
                        OutboxRepository outboxRepository,
                        ObjectMapper objectMapper) {
        this.orderRepository = orderRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional  // CRITICAL: order + outbox event saved in ONE transaction
    public OrderResponse createOrder(OrderRequest request) {
        // 0. Idempotency check — if a key was provided and an order already exists, return it
        if (request.idempotencyKey != null && !request.idempotencyKey.isBlank()) {
            var existing = orderRepository.findByIdempotencyKey(request.idempotencyKey);
            if (existing.isPresent()) {
                return OrderResponse.fromEntity(existing.get());
            }
        }

        // 1. Build the Order entity
        Order order = new Order();
        order.setUserId(request.userId);
        order.setStatus(OrderStatus.PENDING);
        order.setIdempotencyKey(request.idempotencyKey);

        // 2. Map each request item to an OrderItem entity and attach to the order
        for (OrderItemRequest itemReq : request.items) {
            OrderItem item = new OrderItem();
            item.setProductName(itemReq.productName);
            item.setQuantity(itemReq.quantity);
            item.setUnitPrice(itemReq.unitPrice);
            order.addItem(item);
        }

        // 3. Calculate total price server-side
        BigDecimal total = order.getItems().stream()
                .map(item -> item.getUnitPrice().multiply(BigDecimal.valueOf(item.getQuantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        order.setTotalPrice(total);

        // 4. Save the order (cascades to order_items)
        orderRepository.save(order);

        // 5. Write event to outbox — SAME transaction as the order save above.
        OrderEvent event = OrderEvent.orderCreated(order.getId(), order.getUserId(), order.getTotalPrice());
        OutboxEvent outbox = new OutboxEvent();
        outbox.setAggregateId(order.getId().toString());
        outbox.setEventType("ORDER_CREATED");
        outbox.setPartitionKey(order.getUserId());
        outbox.setTopic("order-events");
        outbox.setPayload(toJson(event));
        outboxRepository.save(outbox);

        // 6. Convert entity → response
        return OrderResponse.fromEntity(order);
    }

    @Transactional(readOnly = true)
    public List<OrderResponse> getAllOrders() {
        return orderRepository.findAll().stream()
                .map(OrderResponse::fromEntity)
                .toList();
    }

    @Transactional(readOnly = true)
    public OrderResponse getOrderById(UUID id) {
        Order order = orderRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Order not found: " + id));
        return OrderResponse.fromEntity(order);
    }

    private String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize event to JSON", e);
        }
    }
}
