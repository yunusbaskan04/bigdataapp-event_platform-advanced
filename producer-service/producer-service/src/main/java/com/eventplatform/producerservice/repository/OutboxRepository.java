package com.eventplatform.producerservice.repository;

import com.eventplatform.producerservice.entity.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface OutboxRepository
        extends JpaRepository<OutboxEvent, UUID> {
}
