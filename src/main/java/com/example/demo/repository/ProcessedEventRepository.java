package com.example.demo.repository;

import com.example.demo.model.ProcessedEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, Long> {

    // Spring Data derives: SELECT COUNT(*) > 0 FROM processed_events
    //                      WHERE event_id = ? AND consumer_group = ?
    boolean existsByEventIdAndConsumerGroup(String eventId, String consumerGroup);
}
