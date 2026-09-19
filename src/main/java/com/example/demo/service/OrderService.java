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
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;
    private final TransactionTemplate transactionTemplate;

    public OrderService(OrderRepository orderRepository,
                        OutboxRepository outboxRepository,
                        ObjectMapper objectMapper,
                        TransactionTemplate transactionTemplate) {
        this.orderRepository = orderRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
        this.transactionTemplate = transactionTemplate;
    }

    // Not @Transactional on purpose. The write happens in its own transaction
    // (see persistNewOrder) so that a unique-constraint violation on
    // idempotencyKey can be caught here, AFTER that transaction has rolled back,
    // and answered by re-reading the winning order in a fresh transaction.
    // Doing the catch inside the same transaction would not work: Postgres
    // aborts the transaction on the first error, so the re-read would fail too.
    public OrderResponse createOrder(OrderRequest request) {
        // Blank keys are treated as "no key". Storing "" would make every
        // key-less order collide on the unique constraint (NULLs don't collide, "" does).
        String idempotencyKey = normalizeKey(request.idempotencyKey);

        // 0. Idempotency check — if a key was provided and an order already exists, return it
        if (idempotencyKey != null) {
            Optional<OrderResponse> existing = findByIdempotencyKey(idempotencyKey);
            if (existing.isPresent()) {
                return existing.get();
            }
        }

        try {
            return transactionTemplate.execute(status -> persistNewOrder(request, idempotencyKey));
        } catch (DataIntegrityViolationException e) {
            // Two requests with the same key raced past the check above and both
            // tried to insert. The loser lands here: return the winner's order.
            if (idempotencyKey != null) {
                Optional<OrderResponse> existing = findByIdempotencyKey(idempotencyKey);
                if (existing.isPresent()) {
                    return existing.get();
                }
            }
            throw e;
        }
    }

    // CRITICAL: order + outbox event saved in ONE transaction (the TransactionTemplate above).
    private OrderResponse persistNewOrder(OrderRequest request, String idempotencyKey) {
        // 1. Build the Order entity
        Order order = new Order();
        order.setUserId(request.userId);
        order.setStatus(OrderStatus.PENDING);
        order.setIdempotencyKey(idempotencyKey);

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

        // 4. Save the order (cascades to order_items).
        // saveAndFlush forces the INSERT now, so a duplicate idempotencyKey surfaces
        // here as DataIntegrityViolationException instead of at commit time.
        orderRepository.saveAndFlush(order);

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

    // Runs in its own short transaction so the lazy items collection can be
    // mapped to the response before the session closes.
    private Optional<OrderResponse> findByIdempotencyKey(String idempotencyKey) {
        return Optional.ofNullable(transactionTemplate.execute(status ->
                orderRepository.findByIdempotencyKey(idempotencyKey)
                        .map(OrderResponse::fromEntity)
                        .orElse(null)));
    }

    private static String normalizeKey(String key) {
        return (key == null || key.isBlank()) ? null : key.trim();
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
                .orElseThrow(() -> new OrderNotFoundException(id));
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
