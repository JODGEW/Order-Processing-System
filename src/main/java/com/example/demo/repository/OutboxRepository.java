package com.example.demo.repository;

import com.example.demo.model.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface OutboxRepository extends JpaRepository<OutboxEvent, UUID> {

    // Spring Data derives the query from the method name:
    //   findBy           → SELECT ... WHERE
    //   Published        → published =
    //   False            → false
    //   OrderByCreatedAt → ORDER BY created_at ASC
    //
    // Result: SELECT * FROM outbox WHERE published = false ORDER BY created_at ASC
    List<OutboxEvent> findByPublishedFalseOrderByCreatedAt();
}
