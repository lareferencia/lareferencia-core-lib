package org.lareferencia.core.domain;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import java.time.OffsetDateTime;
import org.lareferencia.core.task.TaskManager.RuntimeConfiguration;

/** Singleton installation override; file properties apply until the first admin save. */
@Entity
@Table(name = "taskmanager_configuration")
@Getter
@Setter
public class TaskManagerConfiguration {
    @Id private Long id;
    @Column(name = "concurrent_tasks", nullable = false) private int concurrentTasks;
    @Column(name = "max_queued_tasks", nullable = false) private int maxQueuedTasks;
    @Column(name = "result_retention_seconds", nullable = false) private long resultRetentionSeconds;
    @Column(name = "max_retained_results", nullable = false) private int maxRetainedResults;
    @Column(name = "shutdown_timeout_seconds", nullable = false) private long shutdownTimeoutSeconds;
    @Column(name = "updated_at", nullable = false) private OffsetDateTime updatedAt;
    @Column(name = "updated_by") private String updatedBy;

    public RuntimeConfiguration toRuntimeConfiguration() {
        return new RuntimeConfiguration(concurrentTasks, maxQueuedTasks, resultRetentionSeconds,
                maxRetainedResults, shutdownTimeoutSeconds);
    }
    public void setRuntimeConfiguration(RuntimeConfiguration value) {
        concurrentTasks = value.concurrentTasks();
        maxQueuedTasks = value.maxQueuedTasks();
        resultRetentionSeconds = value.resultRetentionSeconds();
        maxRetainedResults = value.maxRetainedResults();
        shutdownTimeoutSeconds = value.shutdownTimeoutSeconds();
    }
}
