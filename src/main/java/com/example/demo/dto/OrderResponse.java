package com.example.demo.dto;

import com.example.demo.model.Order;
import com.example.demo.model.OrderItem;
import java.math.BigDecimal;
import java.util.List;

public class OrderResponse {
    public String orderId;
    public String userId;
    public List<OrderItemResponse> items;
    public BigDecimal totalPrice;
    public String status;
    public String createdAt;

    // Static factory method — builds a response from an Order entity.
    // This keeps conversion logic in one place instead of scattered across services.
    public static OrderResponse fromEntity(Order order) {
        OrderResponse response = new OrderResponse();
        response.orderId = order.getId().toString();
        response.userId = order.getUserId();
        response.totalPrice = order.getTotalPrice();
        response.status = order.getStatus().name();
        response.createdAt = order.getCreatedAt().toString();
        response.items = order.getItems().stream()
                .map(OrderItemResponse::fromEntity)
                .toList();
        return response;
    }

    // Nested class — keeps the item response bundled with the order response.
    public static class OrderItemResponse {
        public String productName;
        public Integer quantity;
        public BigDecimal unitPrice;

        public static OrderItemResponse fromEntity(OrderItem item) {
            OrderItemResponse r = new OrderItemResponse();
            r.productName = item.getProductName();
            r.quantity = item.getQuantity();
            r.unitPrice = item.getUnitPrice();
            return r;
        }
    }
}
