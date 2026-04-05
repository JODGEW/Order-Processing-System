package com.example.demo.model;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Entity                     // Tells JPA "this class maps to a database table"
@Table(name = "orders")     // Table name — "order" is a SQL reserved word, so we use "orders"
public class Order {

    @Id                                          // Primary key
    @GeneratedValue(strategy = GenerationType.UUID)  // Auto-generate a UUID on insert
    private UUID id;

    @Column(nullable = false)
    private String userId;

    // Store the enum as a String in the DB column (e.g., "PENDING", "PAID").
    // Without @Enumerated(STRING), JPA defaults to storing the ordinal (0, 1, 2...)
    // which breaks if you reorder the enum values.
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OrderStatus status;

    @Column(nullable = false, precision = 10, scale = 2)
    private BigDecimal totalPrice;

    // Optimistic locking: Hibernate adds "WHERE version = ?" to every UPDATE.
    // If two threads read version=1 and both try to update, only one succeeds.
    // The other gets OptimisticLockException because the version is now 2.
    // This prevents concurrent consumers from silently overwriting each other's changes.
    // Client-generated idempotency key — prevents duplicate orders from network retries.
    // Unique constraint ensures only one order per key.
    @Column(unique = true)
    private String idempotencyKey;

    @Version
    private Integer version;

    @Column(nullable = false, updatable = false)
    private Instant createdAt;

    private Instant updatedAt;

    // One Order has many OrderItems.
    // CascadeType.ALL: when you save/delete an Order, its items are saved/deleted too.
    // orphanRemoval: if you remove an item from the list, it gets deleted from the DB.
    @OneToMany(mappedBy = "order", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<OrderItem> items = new ArrayList<>();

    // JPA lifecycle callbacks — auto-set timestamps
    @PrePersist
    protected void onCreate() {
        this.createdAt = Instant.now();
        this.updatedAt = Instant.now();
    }

    @PreUpdate
    protected void onUpdate() {
        this.updatedAt = Instant.now();
    }

    // --- Transition the order to a new status, enforcing the state machine ---
    // Throws if the transition is invalid (e.g., PAID → PENDING).
    // Combined with @Version, this gives us two layers of protection:
    //   1. State machine: rejects logically invalid transitions
    //   2. Optimistic lock: rejects concurrent modifications
    public void transitionTo(OrderStatus newStatus) {
        if (!this.status.canTransitionTo(newStatus)) {
            throw new IllegalStateException(
                    "Cannot transition order " + id + " from " + status + " to " + newStatus);
        }
        this.status = newStatus;
    }

    // --- Helper method to maintain both sides of the relationship ---
    public void addItem(OrderItem item) {
        items.add(item);
        item.setOrder(this);
    }

    // --- Getters and Setters ---

    public UUID getId() {
        return id;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public OrderStatus getStatus() {
        return status;
    }

    public void setStatus(OrderStatus status) {
        this.status = status;
    }

    public BigDecimal getTotalPrice() {
        return totalPrice;
    }

    public void setTotalPrice(BigDecimal totalPrice) {
        this.totalPrice = totalPrice;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public void setIdempotencyKey(String idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }

    public List<OrderItem> getItems() {
        return items;
    }
}
