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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.flowable.WorkflowService;
import org.lareferencia.core.repository.jpa.NetworkRepository;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration class for network action executor beans.
 * <p>
 * This configuration provides conditional bean creation based on the
 * {@code workflow.engine} property:
 * <ul>
 * <li>{@code workflow.engine=legacy} (default) - Uses TaskManager</li>
 * <li>{@code workflow.engine=flowable} - Uses WorkflowService with BPMN</li>
 * </ul>
 * </p>
 *
 * @author LA Referencia Team
 */
@Configuration
public class NetworkActionExecutorConfig {

    private static final Logger logger = LogManager.getLogger(NetworkActionExecutorConfig.class);

    /**
     * Creates the legacy network action executor using TaskManager.
     * <p>
     * This is the default implementation, activated when:
     * <ul>
     * <li>{@code workflow.engine=legacy}</li>
     * <li>Property is not set (matchIfMissing=true)</li>
     * </ul>
     * </p>
     *
     * @param taskManager        the task manager for scheduling
     * @param applicationContext Spring application context for bean lookup
     * @param networkRepository  repository for network entities
     * @return legacy executor instance
     */
    @Bean
    @ConditionalOnProperty(name = "workflow.engine", havingValue = "legacy", matchIfMissing = true)
    public INetworkActionExecutor legacyNetworkActionExecutor(
            TaskManager taskManager,
            ApplicationContext applicationContext,
            NetworkRepository networkRepository) {

        logger.info("Initializing LEGACY network action executor (TaskManager-based)");
        return new LegacyNetworkActionExecutor(taskManager, applicationContext, networkRepository);
    }

    /**
     * Creates the Flowable-based network action executor using WorkflowService.
     * <p>
     * Activated when: {@code workflow.engine=flowable}
     * </p>
     *
     * @param workflowService   the Flowable workflow service
     * @param networkRepository repository for network entities
     * @return Flowable executor instance
     */
    @Bean
    @ConditionalOnProperty(name = "workflow.engine", havingValue = "flowable")
    public INetworkActionExecutor flowableNetworkActionExecutor(
            WorkflowService workflowService,
            NetworkRepository networkRepository) {

        logger.info("Initializing FLOWABLE network action executor (WorkflowService-based)");
        return new FlowableNetworkActionExecutor(workflowService, networkRepository);
    }
}
