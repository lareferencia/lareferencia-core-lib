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

/** Installation-wide configuration for an allow-listed worker bean. */
@Entity
@Table(name = "application_worker_configuration", uniqueConstraints = @UniqueConstraint(
        name = "uk_application_worker_engine_key", columnNames = { "engine_type", "worker_key" }))
@Getter
@Setter
public class ApplicationWorkerConfiguration {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "engine_type", nullable = false, length = 32)
    private String engineType;

    @Column(name = "worker_key", nullable = false)
    private String workerKey;

    @Column(nullable = false)
    private boolean available;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false, columnDefinition = "jsonb")
    private JsonNode definition;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false, columnDefinition = "jsonb")
    private JsonNode configuration;

    @Column(name = "created_at", nullable = false) private OffsetDateTime createdAt;
    @Column(name = "updated_at", nullable = false) private OffsetDateTime updatedAt;
    @Column(name = "updated_by") private String updatedBy;
    @Column(name = "last_seen_at") private OffsetDateTime lastSeenAt;

    @PrePersist void createTimestamps() { OffsetDateTime now = OffsetDateTime.now(); createdAt = createdAt == null ? now : createdAt; updatedAt = now; }
    @PreUpdate void updateTimestamp() { updatedAt = OffsetDateTime.now(); }
}
