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
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

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
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        scheduler.initialize();
        return scheduler;
    }

    /**
     * Creates TaskManager bean for legacy workflow execution.
     * 
     * @return TaskManager instance
     */
    @Bean
    public TaskManager taskManager() {
        return new TaskManager();
    }
}
