package com.example.demo.model;

import java.util.Map;
import java.util.Set;

// Defines every valid state an order can be in, and which transitions are allowed.
//
// State diagram:
//
//   PENDING ──────► PAID ──────► CANCELLED
//      │                            ▲
//      └──► PAYMENT_FAILED ─────────┘
//               │
//               └──► PENDING  (allow retry)
//
// Why a state machine?
// Without it, any consumer can set any status at any time.
// A bug or race condition could move an order from PAID back to PENDING.
// The state machine rejects that at the domain level — not at the DB level,
// not in a controller, but in the enum itself. Impossible states become
// impossible to represent.
public enum OrderStatus {

    PENDING,
    PAID,
    PAYMENT_FAILED,
    CANCELLED;

    // Each status maps to the set of statuses it's allowed to transition to.
    private static final Map<OrderStatus, Set<OrderStatus>> VALID_TRANSITIONS = Map.of(
        PENDING,         Set.of(PAID, PAYMENT_FAILED, CANCELLED),
        PAID,            Set.of(CANCELLED),
        PAYMENT_FAILED,  Set.of(PENDING),    // allow payment retry
        CANCELLED,       Set.of()            // terminal state — no transitions out
    );

    public boolean canTransitionTo(OrderStatus target) {
        return VALID_TRANSITIONS.getOrDefault(this, Set.of()).contains(target);
    }
}
