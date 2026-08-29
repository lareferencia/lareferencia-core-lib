package org.lareferencia.core.domain;

import java.time.OffsetDateTime;

import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import com.fasterxml.jackson.databind.JsonNode;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lombok.Getter;
import lombok.Setter;

/** Internal installation-level policy for a discovered executable action. */
@Entity
@Table(name = "application_action", uniqueConstraints = @UniqueConstraint(
        name = "uk_application_action_engine_key", columnNames = { "engine_type", "action_key" }))
@Getter
@Setter
public class ApplicationAction {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "engine_type", nullable = false, length = 32)
    private String engineType;

    @Column(name = "action_key", nullable = false)
    private String actionKey;

    @Column(nullable = false)
    private boolean enabled;

    @Column(nullable = false)
    private boolean available;

    /**
     * Installation-controlled execution sequence. Lower values run first.
     * This is deliberately separate from the discovered action definition: the
     * catalogue, rather than XML or BPMN metadata, is the source of truth.
     */
    @Column(name = "execution_order", nullable = false)
    private int executionOrder;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false, columnDefinition = "jsonb")
    private JsonNode definition;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false, columnDefinition = "jsonb")
    private JsonNode configuration;

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    @Column(name = "updated_by")
    private String updatedBy;

    @Column(name = "last_seen_at")
    private OffsetDateTime lastSeenAt;

    @PrePersist
    void createTimestamps() {
        OffsetDateTime now = OffsetDateTime.now();
        createdAt = createdAt == null ? now : createdAt;
        updatedAt = now;
    }

    @PreUpdate
    void updateTimestamp() {
        updatedAt = OffsetDateTime.now();
    }
}
