package org.lareferencia.core.task;

import java.time.OffsetDateTime;
import org.lareferencia.core.domain.TaskManagerConfiguration;
import org.lareferencia.core.repository.jpa.TaskManagerConfigurationRepository;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Serializes admin writes, commits durable settings before applying them to this JVM. */
@Service
@ConditionalOnProperty(name = "workflow.engine", havingValue = "legacy", matchIfMissing = true)
public class TaskManagerRuntimeConfigurationService {
    public static final long CONFIGURATION_ID = 1L;
    private final TaskManager tasks;
    private final TaskManagerConfigurationRepository repository;
    private final TransactionTemplate transactions;

    public record ConfigurationResponse(TaskManager.RuntimeConfiguration configuration, boolean persisted,
            OffsetDateTime updatedAt, String updatedBy) { }

    public TaskManagerRuntimeConfigurationService(TaskManager tasks, TaskManagerConfigurationRepository repository,
            PlatformTransactionManager transactionManager) {
        this.tasks = tasks;
        this.repository = repository;
        this.transactions = new TransactionTemplate(transactionManager);
        this.transactions.setPropagationBehavior(org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    public synchronized ConfigurationResponse get() {
        var row = repository.findById(CONFIGURATION_ID).orElse(null);
        return new ConfigurationResponse(tasks.getRuntimeConfiguration(), row != null,
                row == null ? null : row.getUpdatedAt(), row == null ? null : row.getUpdatedBy());
    }

    public synchronized ConfigurationResponse replace(TaskManager.RuntimeConfiguration value, String updatedBy) {
        var row = transactions.execute(status -> {
            var persisted = repository.findById(CONFIGURATION_ID).orElseGet(TaskManagerConfiguration::new);
            persisted.setId(CONFIGURATION_ID);
            persisted.setRuntimeConfiguration(value);
            persisted.setUpdatedAt(OffsetDateTime.now());
            persisted.setUpdatedBy(updatedBy);
            return repository.saveAndFlush(persisted);
        });
        // A failed DB commit never changes the live limits. A restart restores a committed value.
        tasks.updateRuntimeConfiguration(value);
        return new ConfigurationResponse(value, true, row.getUpdatedAt(), row.getUpdatedBy());
    }
}
