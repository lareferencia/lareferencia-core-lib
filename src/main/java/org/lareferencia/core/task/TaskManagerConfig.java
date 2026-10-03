/*
 *   Copyright (c) 2013-2025. LA Referencia / Red CLARA and others
 *
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU Affero General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU Affero General Public License for more details.
 *
 *   You should have received a copy of the GNU Affero General Public License
 *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *   This file is part of LA Referencia software platform LRHarvester v4.x
 *   For any further information please contact Lautaro Matas <lmatas@gmail.com>
 */

package org.lareferencia.core.task;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import java.time.Clock;
import java.time.Duration;
import org.lareferencia.core.repository.jpa.TaskManagerConfigurationRepository;

/**
 * Configuration for TaskManager bean (legacy workflow engine).
 * 
 * <p>
 * TaskManager is only created when workflow.engine=legacy (or not set).
 * When workflow.engine=flowable, this configuration is skipped.
 * 
 * @author LA Referencia Team
 */
@Configuration
@EnableScheduling
@ConditionalOnProperty(name = "workflow.engine", havingValue = "legacy", matchIfMissing = true)
public class TaskManagerConfig {

    /**
     * Creates TaskScheduler bean for legacy workflow execution.
     * This is the same scheduler defined in XML but created conditionally.
     * 
     * @param poolSize scheduler pool size from properties
     * @return TaskScheduler instance
     */
    @Bean(name = "taskScheduler")
    public TaskScheduler taskScheduler(@Value("${scheduler.pool.size:10}") int poolSize) {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(poolSize);
        scheduler.setThreadNamePrefix("taskScheduler-");
        scheduler.setRemoveOnCancelPolicy(true);
        scheduler.setAwaitTerminationSeconds(5);
        return scheduler;
    }

    /**
     * Creates TaskManager bean for legacy workflow execution.
     * 
     * @return TaskManager instance
     */
    @Bean(name = "taskManager")
    public TaskManager persistedTaskManager(TaskScheduler taskScheduler, Environment environment,
            TaskManagerConfigurationRepository repository) {
        var persisted = repository.findById(TaskManagerRuntimeConfigurationService.CONFIGURATION_ID);
        return createTaskManager(taskScheduler, environment, persisted.map(row -> row.toRuntimeConfiguration()).orElse(null));
    }

    // Kept for callers creating the coordinator outside Spring, without a persistence repository.
    public TaskManager taskManager(TaskScheduler taskScheduler, Environment environment) {
        return createTaskManager(taskScheduler, environment, null);
    }

    private TaskManager createTaskManager(TaskScheduler taskScheduler, Environment environment,
            TaskManager.RuntimeConfiguration persisted) {
        int active = persisted == null ? environment.getProperty("taskmanager.concurrent.tasks", Integer.class, 4) : persisted.concurrentTasks();
        int queued = persisted == null ? queuedLimit(environment) : persisted.maxQueuedTasks();
        long retention = persisted == null ? environment.getProperty("taskmanager.result-retention-seconds", Long.class, 3600L) : persisted.resultRetentionSeconds();
        int retained = persisted == null ? environment.getProperty("taskmanager.max-retained-results", Integer.class, 1000) : persisted.maxRetainedResults();
        long shutdown = persisted == null ? environment.getProperty("taskmanager.shutdown-timeout-seconds", Long.class, 30L) : persisted.shutdownTimeoutSeconds();
        long reconcile = environment.getProperty("taskmanager.reconcile.interval-ms", Long.class, 2000L);
        if (active < 1 || queued < 0 || retention < 1 || retained < 1 || shutdown < 0 || reconcile < 1) {
            throw new IllegalArgumentException("Invalid TaskManager configuration");
        }
        // Validate durations before allocating an executor.
        new TaskManager.RuntimeConfiguration(active, queued, retention, retained, shutdown);
        return new TaskManager(taskScheduler, TaskManager.newWorkerExecutor(active), active, queued,
                Duration.ofSeconds(retention), retained, Duration.ofSeconds(shutdown), Clock.systemUTC());
    }

    static int queuedLimit(Environment environment) {
        Integer canonical = environment.getProperty("taskmanager.max-queued-tasks", Integer.class);
        Integer legacy = environment.getProperty("taskmanager.max_queuded.tasks", Integer.class);
        if (canonical != null && legacy != null && !canonical.equals(legacy)) {
            throw new IllegalArgumentException("Conflicting TaskManager queued-task limits (canonical and legacy alias)");
        }
        return canonical != null ? canonical : legacy != null ? legacy : 32;
    }
}
