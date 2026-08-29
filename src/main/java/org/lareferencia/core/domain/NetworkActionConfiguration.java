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
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lombok.Getter;
import lombok.Setter;

/** Per-network execution policy and configuration for an application action. */
@Entity
@Table(name = "network_action", uniqueConstraints = @UniqueConstraint(
        name = "uk_network_action_network_application", columnNames = { "network_id", "application_action_id" }))
@Getter
@Setter
public class NetworkActionConfiguration {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(optional = false)
    @JoinColumn(name = "network_id", nullable = false)
    private Network network;

    @ManyToOne(optional = false)
    @JoinColumn(name = "application_action_id", nullable = false)
    private ApplicationAction applicationAction;

    @Column(nullable = false)
    private boolean enabled = true;

    @Column(name = "schedule_enabled", nullable = false)
    private boolean scheduleEnabled;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false, columnDefinition = "jsonb")
    private JsonNode configuration;

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    @Column(name = "updated_by")
    private String updatedBy;

    @PrePersist void created() { OffsetDateTime now = OffsetDateTime.now(); createdAt = now; updatedAt = now; }
    @PreUpdate void updated() { updatedAt = OffsetDateTime.now(); }
}
