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
package org.lareferencia.core.flowable.config;

import org.flowable.engine.ProcessEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.stereotype.Component;

/**
 * Handles graceful shutdown of Flowable ProcessEngine.
 * This ensures the async executor and all related threads are properly stopped
 * when the application context is closed.
 * 
 * @author LA Referencia Team
 */
@Component
@ConditionalOnProperty(name = "workflow.engine", havingValue = "flowable")
public class FlowableShutdownHandler implements ApplicationListener<ContextClosedEvent> {

    private static final Logger logger = LoggerFactory.getLogger(FlowableShutdownHandler.class);

    @Autowired(required = false)
    private ProcessEngine processEngine;

    @Override
    public void onApplicationEvent(ContextClosedEvent event) {
        logger.info("Application context closing - shutting down Flowable ProcessEngine");

        if (processEngine != null) {
            try {
                // Force close the process engine
                logger.info("Closing ProcessEngine: {}", processEngine.getName());
                processEngine.close();
                logger.info("ProcessEngine closed successfully");
            } catch (Exception e) {
                logger.warn("Error closing ProcessEngine: {}", e.getMessage());
            }
        } else {
            logger.debug("No ProcessEngine found to close");
        }
    }
}
